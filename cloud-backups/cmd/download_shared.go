package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/relizaio/cloud-backup/internal/config"
	"github.com/relizaio/cloud-backup/internal/pipeline"
	"github.com/relizaio/cloud-backup/internal/storage"
)

// runFileDownload downloads a backup file from cloud storage, applies the given reader
// modifier chain (e.g. age decryption), and writes the result to cfg.OutputFile.
// Used by both `oci download` and `pg download`.
func runFileDownload(ctx context.Context, storeProvider storage.Provider, cfg *config.AppConfig, readerMods []pipeline.ReaderModifier) (err error) {
	slog.Info("download_started", "backup_file", cfg.BackupFile, "output", cfg.OutputFile)

	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	outFile, createErr := os.Create(cfg.OutputFile)
	if createErr != nil {
		slog.Error("failed_to_create_output_file", "error", createErr.Error())
		return fmt.Errorf("failed to create output file %q: %w", cfg.OutputFile, createErr)
	}
	defer func() {
		outFile.Close()
		if err != nil {
			os.Remove(cfg.OutputFile)
		}
	}()

	cloudR, cloudW := io.Pipe()
	errChan := make(chan error, 1)

	go func() {
		if dlErr := storeProvider.DownloadStream(ctx, cfg.BackupFile, cloudW); dlErr != nil {
			cloudW.CloseWithError(dlErr)
			errChan <- dlErr
			return
		}
		cloudW.Close()
		errChan <- nil
	}()

	reader, applyErr := pipeline.ApplyReaderModifiers(cloudR, readerMods)
	if applyErr != nil {
		cloudR.Close()
		<-errChan
		return fmt.Errorf("failed to apply reader modifiers: %w", applyErr)
	}

	if _, copyErr := io.Copy(outFile, reader); copyErr != nil {
		cloudR.Close()
		dlErr := <-errChan
		if dlErr != nil {
			return fmt.Errorf("download failed: %w", dlErr)
		}
		return fmt.Errorf("failed to write output file: %w", copyErr)
	}

	cloudR.Close()
	if dlErr := <-errChan; dlErr != nil {
		return fmt.Errorf("download failed: %w", dlErr)
	}

	slog.Info("download_completed_successfully", "output", cfg.OutputFile)
	return nil
}
