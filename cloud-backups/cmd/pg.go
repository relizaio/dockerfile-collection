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
	pgCmd.PersistentFlags().String("exclude-table", "", "pg backup: comma-separated pg_dump --exclude-table patterns (wildcards ok, e.g. 'rearm.audit_archive_*') to omit from a whole-DB backup -- e.g. the retained audit archive tables, which have their own permanent-bucket backups (ENV: EXCLUDE_TABLE)")

	// audit-rotate specific flags
	pgCmd.PersistentFlags().String("pg-schema", "rearm", "Schema containing the audit table, for audit-rotate (ENV: PG_SCHEMA)")
	pgCmd.PersistentFlags().String("audit-table", "audit", "Audit table name, for audit-rotate (ENV: AUDIT_TABLE)")
	pgCmd.PersistentFlags().Int("audit-retention-days", 30, "audit-rotate: keep each sealed archive on disk (queryable by name for ops inspection) until it is older than N days, then DROP it whole on a later run (0 = drop on the next run) (ENV: AUDIT_RETENTION_DAYS)")
	pgCmd.PersistentFlags().Int("rotation-interval-days", 0, "audit-rotate: rotate (cut a new archive) only when the newest existing archive is >= N days old, decoupling rotation from the cron cadence so a fast cron (per-minute..daily) still yields ~retention/N coexisting archives. 0 = OFF = rotate every run. Set = --audit-retention-days for a single archive at a time. Must be <= --audit-retention-days. (ENV: ROTATION_INTERVAL_DAYS)")
	pgCmd.PersistentFlags().String("lock-timeout", "5s", "audit-rotate: lock_timeout for the rename step; on contention the rotate rolls back and retries next run (ENV: LOCK_TIMEOUT)")
	pgCmd.PersistentFlags().Bool("allow-unencrypted", false, "audit-rotate: allow writing an UNENCRYPTED dump to the permanent bucket when no --encryption-password is set (ENV: ALLOW_UNENCRYPTED)")
	pgCmd.PersistentFlags().Bool("verify-restore", false, "audit-rotate: before an aged-out drop, re-download the archive, decrypt it, run pg_restore -l (proves it's a restorable dump), and match its SHA-256 (full re-download). Default is a cheap existence gate (ENV: VERIFY_RESTORE)")
	pgCmd.PersistentFlags().Bool("drain-backlog", false, "audit-rotate: back up + drop the archive created THIS run immediately, regardless of retention age. Set only on the first/cutover run to reclaim the historical backlog now; retention accumulates from the next run. Keep false for the recurring cron. (ENV: DRAIN_BACKLOG)")
	pgCmd.PersistentFlags().Bool("drop-instance-rows", false, "audit-rotate: proceed even if the audit table holds frozen entity_name='instances' rows (still read by the app but never re-written). Without this the run refuses when such rows exist. Setting it does NOT lose data (the rows are backed up to the permanent bucket like any archive) but the app's instance-revision reads return empty once those rows age out of the DB -- a conscious cutover choice. (ENV: DROP_INSTANCE_ROWS)")

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
	mustBindPFlag("exclude-table", "exclude-table")
}
