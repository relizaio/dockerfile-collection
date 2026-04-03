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

	mustBindPFlag := func(key, flagName string) {
		if err := viper.BindPFlag(key, pgCmd.PersistentFlags().Lookup(flagName)); err != nil {
			panic(fmt.Sprintf("failed to bind pg flag %q: %v", flagName, err))
		}
	}
	mustBindPFlag("pg-host", "pg-host")
	mustBindPFlag("pg-port", "pg-port")
	mustBindPFlag("pg-database", "pg-database")
	mustBindPFlag("pg-user", "pg-user")
}
