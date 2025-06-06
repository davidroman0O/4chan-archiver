package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile     string
	outputDir   string
	rateLimitMs int
	maxRetries  int
	userAgent   string
	verbose     bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "4chan-archiver",
	Short: "A comprehensive 4chan thread archiver",
	Long: `A flexible and controlled 4chan thread archiver that supports:
- One-time archiving of single or multiple threads
- Metadata tracking to avoid duplicate downloads
- Rate limiting to be respectful to servers
- Support for multiple boards (pol, b, etc.)
- Content filtering and organization`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.4chan-archiver.yaml)")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", "", "output directory (default: ~/Documents/4chan-archive)")
	rootCmd.PersistentFlags().IntVar(&rateLimitMs, "rate-limit", 1000, "rate limit between requests in milliseconds")
	rootCmd.PersistentFlags().IntVar(&maxRetries, "max-retries", 3, "maximum number of retries for failed requests")
	rootCmd.PersistentFlags().StringVar(&userAgent, "user-agent", "", "custom user agent (default: random)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose logging")

	// Bind flags to viper
	viper.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))
	viper.BindPFlag("rate-limit", rootCmd.PersistentFlags().Lookup("rate-limit"))
	viper.BindPFlag("max-retries", rootCmd.PersistentFlags().Lookup("max-retries"))
	viper.BindPFlag("user-agent", rootCmd.PersistentFlags().Lookup("user-agent"))
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".4chan-archiver" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".4chan-archiver")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// Set default values
	if outputDir == "" {
		home, _ := os.UserHomeDir()
		viper.SetDefault("output", filepath.Join(home, "Documents", "4chan-archive"))
	}
	viper.SetDefault("rate-limit", 1000)
	viper.SetDefault("max-retries", 3)
	viper.SetDefault("verbose", false)

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil && verbose {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
