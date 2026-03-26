package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

type Downloader struct {
	manager      *Manager
	strmURL      string
	mountPath    string
	dest         string
	logger       zerolog.Logger
	maxDownloads int
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:      manager,
		strmURL:      strmURL,
		mountPath:    cfg.Mount.MountPath,
		logger:       manager.logger.With().Str("component", "downloader").Logger(),
		dest:         cfg.DownloadFolder,
		maxDownloads: cfg.MaxDownloads,
	}
}

func (d *Downloader) download(torrent *storage.Entry) error {
	var (
		isMultiSeason bool
		seasons       []SeasonInfo
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons = d.detectMultiSeason(torrent)
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {

		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			// Then process the symlinks for each season torrent
			if err := d.process(result, torrentMountPath); err != nil {
				d.markAsError(result, err)
			}
		}
	}
	return d.process(torrent, torrentMountPath)
}

func (d *Downloader) process(entry *storage.Entry, mountPath string) error {
	switch entry.Action {
	case config.DownloadActionDownload:
		return d.processDownload(entry)
	case config.DownloadActionSymlink:
		return d.processSymlink(entry, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(entry)
	case config.DownloadActionNone:
		d.markAsCompleted(entry)
		// Remove entry from queue
		_ = d.manager.queue.Delete(entry.InfoHash, nil)
		return nil
	default:
		return d.processSymlink(entry, mountPath)
	}
}

func (d *Downloader) markAsCompleted(entry *storage.Entry) {
	// Mark as completed
	entry.MarkAsCompleted(entry.DownloadPath())
	_ = d.manager.queue.Update(entry)

	// Send notification
	msg := fmt.Sprintf("Download completed: %s [%s] -> %s", entry.Name, entry.Category, entry.DownloadPath())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadComplete,
		Status:  "success",
		Entry:   entry,
		Message: msg,
	})

	// Trigger arr refresh
	go func() {
		a := d.manager.arr.GetOrCreate(entry.Category)
		a.Refresh()
	}()
}

func (d *Downloader) markAsError(entry *storage.Entry, err error) {
	d.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to process action")
	entry.MarkAsError(err)
	_ = d.manager.queue.Update(entry)

	// Send error notification
	msg := fmt.Sprintf("Download failed: %s [%s] - %s", entry.Name, entry.Category, err.Error())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadFailed,
		Status:  "error",
		Entry:   entry,
		Message: msg,
		Error:   err,
	})
}

// processSymlink creates symlinks for torrent files with comprehensive verification
func (d *Downloader) processSymlink(entry *storage.Entry, mountPath string) error {
	files := entry.GetActiveFilesFiltered()
	torrentSymlinkPath := entry.DownloadPath()
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	// Track pending files
	remainingFiles := make(map[string]*storage.File)
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(30 * time.Minute)
	filePaths := make([]string, 0, len(remainingFiles))

	var checkDirectory func(string) // Recursive function
	checkDirectory = func(dirPath string) {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return
		}

		for _, item := range entries {
			entryName := item.Name()
			fullPath := filepath.Join(dirPath, entryName)

			// Check if this matches a remaining file
			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(torrentSymlinkPath, file.Name)

				if err := os.Symlink(fullPath, fileSymlinkPath); err == nil || os.IsExist(err) {
					filePaths = append(filePaths, fileSymlinkPath)
					delete(remainingFiles, entryName)
					d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), file.Name)
				}
			} else if item.IsDir() {
				// If not found and it's a directory, check inside
				checkDirectory(fullPath)
			}
		}
	}

	for len(remainingFiles) > 0 {
		select {
		case <-ticker.C:
			checkDirectory(mountPath)

		case <-timeout:
			return fmt.Errorf("timeout waiting for files: %d files still pending", len(remainingFiles))
		}
	}

	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	// FIX #1: Make ffprobe mandatory for completion (cannot be disabled)
	// This ensures Radarr/Sonarr media info checks will succeed
	if len(filePaths) > 0 {
		probeFiles := filePaths
		if len(probeFiles) > MaxNZBPreCacheFiles {
			probeFiles = probeFiles[:MaxNZBPreCacheFiles]
		}

		d.logger.Info().Int("files", len(probeFiles)).Msgf("Running mandatory ffprobe verification on %s", entry.Name)

		// Run ffprobe - this now BLOCKS completion on failure
		if err := d.manager.RunFFprobe(probeFiles); err != nil {
			return fmt.Errorf("ffprobe verification failed for %s: %w (download not complete - Radarr/Sonarr import would fail)", entry.Name, err)
		}

		d.logger.Info().Str("entry", entry.Name).Msgf("Successfully verified %d/%d files with ffprobe", len(probeFiles), len(filePaths))
	}

	// FIX #2: Verify symlinks are valid and point to correct files
	d.logger.Info().Msgf("Verifying symlink targets for %s", entry.Name)
	for _, filePath := range filePaths {
		if err := d.verifySymlinkTarget(filePath); err != nil {
			return fmt.Errorf("symlink verification failed for %s: %w", filePath, err)
		}
	}
	d.logger.Info().Msgf("All %d symlink targets verified", len(filePaths))

	// FIX #3: Add file readability buffer and re-check
	// Wait for mount to stabilize after ffprobe
	d.logger.Debug().Msgf("Waiting for mount to stabilize before final verification")
	bufferTime := 2 * time.Second
	if entry.IsNZB() {
		bufferTime = 3 * time.Second // NZB might need more time
	}
	time.Sleep(bufferTime)

	// Spot check first few files to ensure they're still readable
	sampleSize := len(filePaths)
	if sampleSize > 5 {
		sampleSize = 5
	}
	d.logger.Debug().Int("sample", sampleSize).Msgf("Running post-buffer readability check")

	for i := 0; i < sampleSize; i++ {
		if err := d.quickFileCheck(filePaths[i]); err != nil {
			return fmt.Errorf("post-buffer file check failed for %s: %w (mount may be unstable)", filePaths[i], err)
		}
	}

	// FIX #2 (continued): Verify media info is readable (mimics Radarr/Sonarr behavior)
	d.logger.Info().Msgf("Running Radarr/Sonarr-compatible media info verification")
	// Check a sample of files (not all, to save time)
	mediaCheckSample := len(filePaths)
	if mediaCheckSample > 3 {
		mediaCheckSample = 3
	}

	for i := 0; i < mediaCheckSample; i++ {
		if err := d.verifyMediaInfoReadable(filePaths[i]); err != nil {
			return fmt.Errorf("media info verification failed for %s: %w (Radarr/Sonarr import would fail)", filePaths[i], err)
		}
	}
	d.logger.Info().Int("checked", mediaCheckSample).Msgf("Media info verification passed")

	d.markAsCompleted(entry)
	d.logger.Info().Msgf("Download completed with full verification: %s", entry.Name)

	return nil
}

// processDownload downloads all files for an entry with progress tracking
// For torrents: uses HTTP download from debrid
// For NZBs: uses parallel NNTP segment download
func (d *Downloader) processDownload(entry *storage.Entry) error {
	// Check if this is a usenet entry
	if entry.IsNZB() {
		return d.processUsenetDownload(entry)
	}
	return d.processTorrentDownload(entry)
}

// processTorrentDownload downloads files from debrid via HTTP
func (d *Downloader) processTorrentDownload(entry *storage.Entry) error {
	files := entry.GetActiveFilesFiltered()
	d.logger.Info().Msgf("Downloading %d files...", len(files))

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}
	entry.SizeDownloaded = 0
	entry.IsDownloading = true
	entry.Progress = 0

	var progressMu sync.Mutex
	progressCallback := func(downloaded int64, speed int64) {
		progressMu.Lock()
		defer progressMu.Unlock()

		entry.SizeDownloaded += downloaded
		entry.Speed = speed
		if totalSize > 0 {
			entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
		}
		entry.UpdatedAt = time.Now()
		_ = d.manager.queue.Update(entry)
	}

	// Resolve download links before spawning goroutines
	type downloadTask struct {
		file *storage.File
		link string
	}
	var tasks []downloadTask
	for _, file := range files {
		downloadLink, err := d.manager.linkService.GetLink(context.Background(), entry, file.Name)
		if err != nil {
			d.logger.Error().Msgf("Failed to get download link for %s: %v", file.Name, err)
			continue
		}
		tasks = append(tasks, downloadTask{file: file, link: downloadLink.DownloadLink})
	}

	p := pool.New().WithErrors().WithFirstError().WithMaxGoroutines(d.maxDownloads)
	for _, task := range tasks {
		p.Go(func() error {
			if err := d.localDownloader(
				task.link,
				filepath.Join(downloadedFolder, task.file.Name),
				task.file.ByteRange,
				progressCallback,
			); err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", task.file.Name, err)
				return err
			}
			d.logger.Info().Msgf("Downloaded %s", task.file.Name)
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	d.markAsCompleted(entry)
	d.logger.Info().Msgf("Downloaded all files for %s", entry.Name)
	return nil
}

// processUsenetDownload downloads NZB files via parallel NNTP segment fetching
func (d *Downloader) processUsenetDownload(entry *storage.Entry) error {
	if d.manager.usenet == nil {
		return fmt.Errorf("usenet client not configured")
	}

	files := entry.GetActiveFilesFiltered()
	d.logger.Info().Msgf("Downloading %d NZB files via usenet...", len(files))

	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}

	entry.SizeDownloaded = 0
	entry.Progress = 0
	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	var progressMu sync.Mutex
	// Track per-file progress so we can compute the global total across all files
	fileProgress := make(map[string]int64)

	p := pool.New().WithErrors().WithFirstError().WithMaxGoroutines(d.maxDownloads)
	for _, file := range files {
		p.Go(func() error {
			destPath := filepath.Join(downloadedFolder, file.Name)
			destFile, err := os.Create(destPath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", file.Name, err)
			}
			defer destFile.Close()

			progressCallback := func(downloaded int64, speed int64) {
				progressMu.Lock()
				defer progressMu.Unlock()

				prev := fileProgress[file.Name]
				fileProgress[file.Name] = downloaded
				entry.SizeDownloaded += downloaded - prev
				entry.Speed = speed
				if totalSize > 0 {
					entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
				}
				entry.UpdatedAt = time.Now()
				_ = d.manager.queue.Update(entry)
			}

			if err := d.manager.usenet.Download(d.manager.ctx, entry.InfoHash, file.Name, destFile, progressCallback); err != nil {
				_ = os.Remove(destPath)
				return fmt.Errorf("failed to download %s: %w", file.Name, err)
			}

			d.logger.Info().Msgf("Downloaded NZB file: %s", file.Name)
			return nil
		})
	}

	err := p.Wait()

	if err != nil {
		entry.MarkAsError(err)
		_ = d.manager.queue.Update(entry)
		return fmt.Errorf("NZB download failed: %w", err)
	}

	d.markAsCompleted(entry)
	d.logger.Info().Msgf("Downloaded all NZB files for %s", entry.Name)
	return nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(torrent *storage.Entry) error {
	files := torrent.GetActiveFilesFiltered()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	torrentSymlinkPath := torrent.DownloadPath()

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	for _, file := range files {
		strmFilePath := filepath.Join(torrentSymlinkPath, file.Name+".strm")
		streamURL, err := url.JoinPath(
			d.strmURL,
			"webdav",
			"stream",
			EntryAllFolder,
			url.PathEscape(torrent.GetFolder()),
			url.PathEscape(file.Name),
		)
		if err != nil {
			continue
		}
		if err := os.WriteFile(strmFilePath, []byte(streamURL), 0644); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.markAsCompleted(torrent)
	d.logger.Info().Str("destination", torrentSymlinkPath).Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) detectMultiSeason(torrent *storage.Entry) (bool, []SeasonInfo) {
	torrentName := torrent.Name
	files := torrent.GetActiveFilesFiltered()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons
}

// localDownloader downloads a file via HTTP with progress reporting
func (d *Downloader) localDownloader(downloadURL, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	req, err := http.NewRequestWithContext(d.manager.ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Decypharr[QBitTorrent]")

	// Set byte range if specified
	if byterange != nil {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byterange[0], byterange[1]))
	}

	resp, err := d.manager.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status %d for %s", resp.StatusCode, downloadURL)
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Use a progress-tracking writer that reports every 2 seconds
	var downloaded atomic.Int64
	startTime := time.Now()
	var lastReported int64

	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(f, io.TeeReader(resp.Body, &countWriter{n: &downloaded}))
		done <- err
	}()

	for {
		select {
		case <-t.C:
			current := downloaded.Load()
			elapsed := time.Since(startTime).Seconds()
			speed := int64(0)
			if elapsed > 0 {
				speed = int64(float64(current) / elapsed)
			}
			if current != lastReported && progressCallback != nil {
				progressCallback(current-lastReported, speed)
				lastReported = current
			}
		case err := <-done:
			// Report final bytes
			if progressCallback != nil {
				final := downloaded.Load()
				if final != lastReported {
					progressCallback(final-lastReported, 0)
				}
			}
			return err
		}
	}
}

// countWriter is a minimal io.Writer that atomically counts bytes written
type countWriter struct {
	n *atomic.Int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n := len(p)
	cw.n.Add(int64(n))
	return n, nil
}

// verifyMediaInfoReadable checks if a file is readable by ffprobe (mimics Radarr/Sonarr behavior)
// This ensures that when Radarr/Sonarr try to read media info, it will succeed
func (d *Downloader) verifyMediaInfoReadable(symlinkPath string) error {
	// Check if ffprobe is available
	if _, err := exec.LookPath("ffprobe"); err != nil {
		d.logger.Warn().Msg("ffprobe not available, skipping media info verification")
		return nil
	}

	// Run ffprobe with same parameters Radarr/Sonarr use
	ctx, cancel := context.WithTimeout(context.Background(), FFprobeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		symlinkPath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffprobe failed for %s: %w\noutput: %s", symlinkPath, err, string(output))
	}

	// Verify we got valid JSON with streams (Radarr/Sonarr expect this)
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		return fmt.Errorf("ffprobe returned invalid JSON for %s: %w", symlinkPath, err)
	}

	streams, ok := result["streams"]
	if !ok {
		return fmt.Errorf("ffprobe output missing streams for %s", symlinkPath)
	}

	streamsArray, ok := streams.([]interface{})
	if !ok || len(streamsArray) == 0 {
		return fmt.Errorf("ffprobe output has no streams for %s", symlinkPath)
	}

	d.logger.Debug().Str("file", symlinkPath).Int("streams", len(streamsArray)).Msg("Media info verification passed")
	return nil
}

// verifySymlinkTarget checks that the symlink points to a valid, readable file
func (d *Downloader) verifySymlinkTarget(symlinkPath string) error {
	// Check if symlink exists
	if _, err := os.Lstat(symlinkPath); err != nil {
		return fmt.Errorf("symlink does not exist: %w", err)
	}

	// Resolve to final target (follows all symlinks)
	resolvedPath, err := filepath.EvalSymlinks(symlinkPath)
	if err != nil {
		return fmt.Errorf("failed to resolve symlink target: %w", err)
	}

	// Verify target file exists and is accessible
	fileInfo, err := os.Stat(resolvedPath)
	if err != nil {
		return fmt.Errorf("symlink target not accessible: %s -> %s: %w", symlinkPath, resolvedPath, err)
	}

	// Verify it's a regular file (not directory or special file)
	if !fileInfo.Mode().IsRegular() {
		return fmt.Errorf("symlink target is not a regular file: %s", resolvedPath)
	}

	// Verify file is readable by attempting to open it
	f, err := os.Open(resolvedPath)
	if err != nil {
		return fmt.Errorf("symlink target not readable: %w", err)
	}
	f.Close()

	return nil
}

// quickFileCheck performs a fast readability check on a file
func (d *Downloader) quickFileCheck(filePath string) error {
	// Try to open and read first 1KB
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("file not openable: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 1024)
	n, err := f.Read(buf)
	if err != nil {
		return fmt.Errorf("file not readable: %w", err)
	}

	if n == 0 {
		return fmt.Errorf("file is empty")
	}

	return nil
}
