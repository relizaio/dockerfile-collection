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
}
