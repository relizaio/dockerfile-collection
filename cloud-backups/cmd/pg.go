package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var pgCmd = &cobra.Command{
	Use:   "pg",
	Short: "Back up and restore PostgreSQL databases",
}

func init() {
	rootCmd.AddCommand(pgCmd)

	// PG-specific persistent flags
	pgCmd.PersistentFlags().String("pg-host", "", "PostgreSQL host, optionally host:port (ENV: PG_HOST)")
	pgCmd.PersistentFlags().String("pg-port", "5432", "PostgreSQL port (ENV: PG_PORT)")
	pgCmd.PersistentFlags().String("pg-database", "", "Database name (ENV: PG_DATABASE)")
	pgCmd.PersistentFlags().String("pg-user", "", "PostgreSQL username (ENV: PG_USER)")
	pgCmd.PersistentFlags().String("exclude-table", "", "pg backup: comma-separated pg_dump --exclude-table patterns (wildcards ok, e.g. 'rearm.audit_archive_*') to omit from a whole-DB backup. NOTE for audit-rotate archives: they are dumped to the permanent bucket only when they AGE OUT, so excluding them means a retained archive exists in exactly one place for up to --audit-retention-days (ENV: EXCLUDE_TABLE)")

	// audit-rotate specific flags
	pgCmd.PersistentFlags().String("pg-schema", "rearm", "Schema containing the audit table, for audit-rotate (ENV: PG_SCHEMA)")
	pgCmd.PersistentFlags().String("audit-table", "audit", "Audit table name, for audit-rotate (ENV: AUDIT_TABLE)")
	pgCmd.PersistentFlags().Int("audit-retention-days", 30, "audit-rotate: keep each sealed archive on disk (queryable by name for ops inspection) until it is older than N days, then back it up and DROP it whole on a later run (0 = back up and drop on the next run) (ENV: AUDIT_RETENTION_DAYS)")
	pgCmd.PersistentFlags().Int("rotation-interval-days", 0, "audit-rotate: rotate (cut a new archive) only when the newest existing archive is >= N days old, decoupling rotation from the cron cadence so a fast cron (per-minute..daily) still yields ~retention/N coexisting archives. 0 = OFF = rotate every run. Set = --audit-retention-days for a single archive at a time. Must be <= --audit-retention-days. (ENV: ROTATION_INTERVAL_DAYS)")
	pgCmd.PersistentFlags().String("lock-timeout", "5s", "audit-rotate: lock_timeout for the rename step; on contention the rotate rolls back and retries next run (ENV: LOCK_TIMEOUT)")
	pgCmd.PersistentFlags().Bool("allow-unencrypted", false, "audit-rotate: allow writing an UNENCRYPTED dump to the permanent bucket when no --encryption-password is set (ENV: ALLOW_UNENCRYPTED)")
	pgCmd.PersistentFlags().Bool("verify-restore", false, "audit-rotate: before an aged-out drop, re-download the archive, decrypt it, run pg_restore -l (proves it's a restorable dump), and match its SHA-256 (full re-download; needs read access on the bucket). By default the run performs NO bucket read at all (ENV: VERIFY_RESTORE)")
	pgCmd.PersistentFlags().Bool("drain-backlog", false, "audit-rotate: back up + drop the archive created THIS run immediately, regardless of retention age. Set only on the first/cutover run to reclaim the historical backlog now; retention accumulates from the next run. Keep false for the recurring cron. (ENV: DRAIN_BACKLOG)")
	pgCmd.PersistentFlags().Bool("drop-instance-rows", false, "audit-rotate: proceed even if the audit table holds frozen entity_name='instances' rows (still read by the app but never re-written). Without this the run refuses when such rows exist. Setting it does NOT lose data (the rows are backed up to the permanent bucket like any archive) but the app's instance-revision reads return empty once those rows age out of the DB -- a conscious cutover choice. (ENV: DROP_INSTANCE_ROWS)")
	pgCmd.PersistentFlags().Int("keep-tail-days", 0, "audit-rotate: after rotating, seed the fresh table with the most recent N days of rows from the just-sealed archive (by --keep-tail-column), so a live reader that queries the live table over a rolling date window keeps its lookback across the rotation boundary. 0 (default) = pure rotation, fresh table starts empty -- correct for a write-only table (audit). Set > the reader's lookback + cron period + margin for a table with such a reader (metrics_audit: the finding-change repair sweep, ~2d lookback + daily cron -> 4). The seed runs inside the rotate transaction, so the ACCESS EXCLUSIVE window becomes O(rows copied) not catalog-only; keep it small. Consecutive archives then OVERLAP by ~N days, so an archive-union restore must dedupe by PK (idempotent consumers are unaffected). Refuses on a STORED generated column (INSERT SELECT * can't write it). Must be <= --audit-retention-days. (ENV: KEEP_TAIL_DAYS)")
	pgCmd.PersistentFlags().String("keep-tail-column", "revision_created_date", "audit-rotate: the timestamptz/date column --keep-tail-days filters on (must be the SAME column the live reader's rolling window queries). Only used when --keep-tail-days > 0. (ENV: KEEP_TAIL_COLUMN)")

	mustBindPFlag := func(key, flagName string) {
		if err := viper.BindPFlag(key, pgCmd.PersistentFlags().Lookup(flagName)); err != nil {
			panic(fmt.Sprintf("failed to bind pg flag %q: %v", flagName, err))
		}
	}
	mustBindPFlag("pg-host", "pg-host")
	mustBindPFlag("pg-port", "pg-port")
	mustBindPFlag("pg-database", "pg-database")
	mustBindPFlag("pg-user", "pg-user")
	mustBindPFlag("pg-schema", "pg-schema")
	mustBindPFlag("audit-table", "audit-table")
	mustBindPFlag("audit-retention-days", "audit-retention-days")
	mustBindPFlag("rotation-interval-days", "rotation-interval-days")
	mustBindPFlag("lock-timeout", "lock-timeout")
	mustBindPFlag("allow-unencrypted", "allow-unencrypted")
	mustBindPFlag("verify-restore", "verify-restore")
	mustBindPFlag("drain-backlog", "drain-backlog")
	mustBindPFlag("drop-instance-rows", "drop-instance-rows")
	mustBindPFlag("keep-tail-days", "keep-tail-days")
	mustBindPFlag("keep-tail-column", "keep-tail-column")
	mustBindPFlag("exclude-table", "exclude-table")
}
