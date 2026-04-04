package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ociCmd = &cobra.Command{
	Use:   "oci",
	Short: "Back up and restore OCI registry artifacts",
}

func init() {
	rootCmd.AddCommand(ociCmd)

	// OCI-specific persistent flags
	ociCmd.PersistentFlags().String("registry-host", "", "Target OCI registry domain (ENV: REGISTRY_HOST)")
	ociCmd.PersistentFlags().String("registry-username", "", "Registry authentication username (ENV: REGISTRY_USERNAME)")
	ociCmd.PersistentFlags().String("registry-token", "", "Registry authentication token (ENV: REGISTRY_TOKEN)")
	ociCmd.PersistentFlags().Int("max-concurrent-jobs", 3, "Number of simultaneous streams (ENV: MAX_CONCURRENT_JOBS)")
	ociCmd.PersistentFlags().Bool("plain-http", false, "Use plain HTTP instead of HTTPS for the registry (ENV: PLAIN_HTTP)")

	mustBindPFlag := func(key, flagName string) {
		if err := viper.BindPFlag(key, ociCmd.PersistentFlags().Lookup(flagName)); err != nil {
			panic(fmt.Sprintf("failed to bind oci flag %q: %v", flagName, err))
		}
	}
	mustBindPFlag("registry-host", "registry-host")
	mustBindPFlag("registry-username", "registry-username")
	mustBindPFlag("registry-token", "registry-token")
	mustBindPFlag("max-concurrent-jobs", "max-concurrent-jobs")
	mustBindPFlag("plain-http", "plain-http")
}
