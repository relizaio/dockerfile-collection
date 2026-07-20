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

	// audit-rotate specific flags
	pgCmd.PersistentFlags().String("pg-schema", "rearm", "Schema containing the audit table, for audit-rotate (ENV: PG_SCHEMA)")
	pgCmd.PersistentFlags().String("audit-table", "audit", "Audit table name, for audit-rotate (ENV: AUDIT_TABLE)")
	pgCmd.PersistentFlags().Int("keep-tail-days", 0, "audit-rotate: also keep audit rows newer than N days in the live table (0 = readers only) (ENV: KEEP_TAIL_DAYS)")
	pgCmd.PersistentFlags().String("lock-timeout", "5s", "audit-rotate: lock_timeout for the rename step; on contention the rotate rolls back and retries next run (ENV: LOCK_TIMEOUT)")
	pgCmd.PersistentFlags().Bool("allow-unencrypted", false, "audit-rotate: allow writing an UNENCRYPTED dump to the permanent bucket when no --encryption-password is set (ENV: ALLOW_UNENCRYPTED)")
	pgCmd.PersistentFlags().Bool("no-drop", false, "audit-rotate: rotate + back up + verify, but do NOT drop the archive (leave it for manual confirmation, then a later run drops it) (ENV: NO_DROP)")
	pgCmd.PersistentFlags().Bool("verify-restore", false, "audit-rotate: before dropping, re-download the archive, decrypt it, run pg_restore -l (proves it's a restorable dump), and match its SHA-256 (full re-download) (ENV: VERIFY_RESTORE)")
	pgCmd.PersistentFlags().Bool("drop-pending", false, "audit-rotate: do NOT rotate; instead verify each already-backed-up leftover archive against its stored .sha256 sidecar (+ pg_restore -l) and drop it. The confirm step after a --no-drop run. (ENV: DROP_PENDING)")

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
	mustBindPFlag("keep-tail-days", "keep-tail-days")
	mustBindPFlag("lock-timeout", "lock-timeout")
	mustBindPFlag("allow-unencrypted", "allow-unencrypted")
	mustBindPFlag("no-drop", "no-drop")
	mustBindPFlag("verify-restore", "verify-restore")
	mustBindPFlag("drop-pending", "drop-pending")
}
