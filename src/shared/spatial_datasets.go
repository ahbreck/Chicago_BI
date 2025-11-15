package shared

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// SpatialDataset describes a spatial dataset that can be downloaded and cached locally.
type SpatialDataset struct {
	Name     string
	URL      string
	FileName string
}

// DefaultSpatialDatasets enumerates the spatial files required by reporting workflows.
var DefaultSpatialDatasets = []SpatialDataset{
	{
		Name:     "neighborhoods",
		URL:      "https://data.cityofchicago.org/resource/igwz-8jzy.geojson",
		FileName: "neighborhoods.geojson",
	},
}

const (
	// spatialDefaultDir is the relative path used when SPATIAL_DATA_DIR is not set.
	spatialDefaultDir = "data/spatial"
	// spatialRequestTimeout bounds the amount of time spent downloading a dataset.
	spatialRequestTimeout = 30 * time.Second
)

// EnsureSpatialDatasets ensures all provided datasets exist on disk, downloading missing files.
// The returned map contains dataset names mapped to their absolute file paths.
func EnsureSpatialDatasets(ctx context.Context, datasets ...SpatialDataset) (map[string]string, error) {
	if len(datasets) == 0 {
		return map[string]string{}, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	dir := os.Getenv("SPATIAL_DATA_DIR")
	if dir == "" {
		dir = spatialDefaultDir
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve spatial data directory: %w", err)
	}

	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create spatial data directory %q: %w", absDir, err)
	}

	client := &http.Client{Timeout: spatialRequestTimeout}
	results := make(map[string]string, len(datasets))
	for _, ds := range datasets {
		if ds.Name == "" {
			return nil, errors.New("dataset name is required")
		}
		if ds.URL == "" {
			return nil, fmt.Errorf("dataset %q is missing a URL", ds.Name)
		}
		if ds.FileName == "" {
			return nil, fmt.Errorf("dataset %q is missing a file name", ds.Name)
		}

		path, err := ensureSpatialDataset(ctx, client, absDir, ds)
		if err != nil {
			return nil, fmt.Errorf("failed to ensure dataset %q: %w", ds.Name, err)
		}
		results[ds.Name] = path
	}

	return results, nil
}

func ensureSpatialDataset(ctx context.Context, client *http.Client, dir string, ds SpatialDataset) (string, error) {
	targetPath := filepath.Join(dir, ds.FileName)
	if info, err := os.Stat(targetPath); err == nil && info.Size() > 0 {
		return targetPath, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ds.URL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to construct request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download %s: %w", ds.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status downloading %s: %s", ds.URL, resp.Status)
	}

	tmpFile, err := os.CreateTemp(dir, ds.FileName+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	wrote := false
	defer func() {
		tmpFile.Close()
		if !wrote {
			os.Remove(tmpFile.Name())
		}
	}()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return "", fmt.Errorf("failed to save dataset contents: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		return "", fmt.Errorf("failed to flush dataset file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close dataset file: %w", err)
	}

	if err := os.Rename(tmpFile.Name(), targetPath); err != nil {
		return "", fmt.Errorf("failed to move dataset into place: %w", err)
	}
	wrote = true

	if err := os.Chmod(targetPath, 0o644); err != nil {
		return "", fmt.Errorf("failed to set permissions on %s: %w", targetPath, err)
	}

	return targetPath, nil
}
