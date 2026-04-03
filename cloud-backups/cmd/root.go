package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "cloud-backup",
	Short: "Unified cloud backup tool for OCI registries and PostgreSQL databases",
	Long:  `cloud-backup streams data directly to AWS S3 or Azure Blob Storage with inline compression and age encryption. Supports OCI artifact backups and PostgreSQL database backups.`,
}

// mustGetString reads a local Cobra flag value directly, bypassing Viper.
// Use this for flags local to a specific subcommand to avoid Viper key collisions
// when multiple subcommands register flags under the same key name.
// Falls back to the corresponding environment variable (FLAG_NAME → FLAG_NAME uppercased,
// hyphens replaced by underscores) when the flag was not explicitly set.
func mustGetString(cmd *cobra.Command, name string) string {
	if f := cmd.Flags().Lookup(name); f != nil && f.Changed {
		return f.Value.String()
	}
	envKey := strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	return os.Getenv(envKey)
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// 1. Tell Viper to read Environment Variables automatically
	viper.AutomaticEnv()
	// Replace hyphens with underscores in env vars (e.g. backup-storage-type -> BACKUP_STORAGE_TYPE)
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// 2. Shared persistent flags (available to all subcommands)
	rootCmd.PersistentFlags().String("backup-storage-type", "s3", "Destination cloud provider: s3 or azure (ENV: BACKUP_STORAGE_TYPE)")
	rootCmd.PersistentFlags().String("encryption-password", "", "Password for age encryption (ENV: ENCRYPTION_PASSWORD)")
	rootCmd.PersistentFlags().String("dump-prefix", "backup", "Prefix attached to the final backup filename (ENV: DUMP_PREFIX)")
	rootCmd.PersistentFlags().String("timeout", "2h", "Per-job stream timeout, e.g. 2h, 90m (ENV: TIMEOUT)")

	// AWS S3 credentials
	rootCmd.PersistentFlags().String("aws-bucket", "", "S3 bucket name (ENV: AWS_BUCKET)")
	rootCmd.PersistentFlags().String("aws-region", "", "AWS region (ENV: AWS_REGION)")
	rootCmd.PersistentFlags().String("aws-access-key-id", "", "AWS access key ID (ENV: AWS_ACCESS_KEY_ID)")
	rootCmd.PersistentFlags().String("aws-secret-access-key", "", "AWS secret access key (ENV: AWS_SECRET_ACCESS_KEY)")

	// Azure Blob Storage credentials
	rootCmd.PersistentFlags().String("azure-storage-account", "", "Azure storage account name (ENV: AZURE_STORAGE_ACCOUNT)")
	rootCmd.PersistentFlags().String("azure-tenant-id", "", "Azure tenant ID (ENV: AZURE_TENANT_ID)")
	rootCmd.PersistentFlags().String("azure-client-id", "", "Azure client ID (ENV: AZURE_CLIENT_ID)")
	rootCmd.PersistentFlags().String("azure-client-secret", "", "Azure client secret (ENV: AZURE_CLIENT_SECRET)")
	rootCmd.PersistentFlags().String("azure-container", "", "Azure blob container name (ENV: AZURE_CONTAINER)")

	// 3. Bind shared flags to Viper
	mustBindPFlag := func(key, flagName string) {
		if err := viper.BindPFlag(key, rootCmd.PersistentFlags().Lookup(flagName)); err != nil {
			panic(fmt.Sprintf("failed to bind flag %q: %v", flagName, err))
		}
	}
	mustBindPFlag("backup-storage-type", "backup-storage-type")
	mustBindPFlag("encryption-password", "encryption-password")
	mustBindPFlag("dump-prefix", "dump-prefix")
	mustBindPFlag("timeout", "timeout")
	mustBindPFlag("aws-bucket", "aws-bucket")
	mustBindPFlag("aws-region", "aws-region")
	mustBindPFlag("aws-access-key-id", "aws-access-key-id")
	mustBindPFlag("aws-secret-access-key", "aws-secret-access-key")
	mustBindPFlag("azure-storage-account", "azure-storage-account")
	mustBindPFlag("azure-tenant-id", "azure-tenant-id")
	mustBindPFlag("azure-client-id", "azure-client-id")
	mustBindPFlag("azure-client-secret", "azure-client-secret")
	mustBindPFlag("azure-container", "azure-container")
}
