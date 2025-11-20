// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/jeremyhahn/go-objstore/pkg/cli"
)

var (
	cfgFile      string
	viperConfig  *viper.Viper
	globalConfig *cli.Config
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "objstore",
	Short: "A CLI tool for managing object storage",
	Long: `objstore is a CLI tool for managing object storage across multiple backends.

Supported Storage Backends:
  - local      : Local filesystem storage
  - s3         : AWS S3
  - minio      : MinIO (S3-compatible)
  - gcs        : Google Cloud Storage
  - azure      : Azure Blob Storage

Archive Backends (for archiving to separate storage):
  - local        : Local filesystem (for archiving to different directory/mount)
  - glacier      : AWS Glacier (archive-only, not for direct storage)
  - azurearchive : Azure Archive Tier (archive-only, not for direct storage)

Configuration can be provided via:
  - Command-line flags (highest priority)
  - Environment variables (OBJECTSTORE_*)
  - Configuration file (~/.objstore.yaml or ./objstore.yaml)
  - Default values (lowest priority)`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize viper configuration
		var err error
		viperConfig, err = cli.InitConfig(cfgFile)
		if err != nil {
			return err
		}

		// Bind flags to viper
		if err := viperConfig.BindPFlags(cmd.Flags()); err != nil {
			return fmt.Errorf("failed to bind flags: %w", err)
		}

		// Get the configuration
		globalConfig = cli.GetConfig(viperConfig)

		return nil
	},
}

var putCmd = &cobra.Command{
	Use:   "put <source-file> <destination-key>",
	Short: "Upload a file to object storage",
	Long: `Upload a file to the object storage backend with the specified key.
Use '-' as the source-file to read from stdin.
You can also set metadata using flags: --content-type, --content-encoding, --custom.`,
	Example: `  objstore put file.txt myfile.txt                                    # Upload local file
  objstore put file.txt prefix/myfile.txt                             # Upload with prefix/path
  cat file.txt | objstore put - myfile.txt                            # Upload from stdin
  objstore put file.txt myfile.txt --content-type application/json    # Upload with content type
  objstore put file.txt myfile.txt --custom author=me,version=1.0     # Upload with custom metadata`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		filePath := args[0]
		key := args[1]

		// Get metadata flags
		contentType, _ := cmd.Flags().GetString("content-type")         //nolint:errcheck // flags are validated by cobra
		contentEncoding, _ := cmd.Flags().GetString("content-encoding") //nolint:errcheck // flags are validated by cobra
		customFields, _ := cmd.Flags().GetStringToString("custom")      //nolint:errcheck // flags are validated by cobra

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.PutCommandWithMetadata(key, filePath, contentType, contentEncoding, customFields); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		// Format success message based on input source
		var message string
		if filePath == "" || filePath == "-" {
			message = fmt.Sprintf("Successfully uploaded data from stdin as '%s'", key)
		} else {
			message = fmt.Sprintf("Successfully uploaded '%s' as '%s'", filePath, key)
		}

		result := &cli.OperationResult{
			Success: true,
			Message: message,
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get <key> [output-file]",
	Short: "Download a file from object storage or get its metadata",
	Long: `Download a file from the object storage backend or retrieve its metadata.
If output-file is not specified or is '-', the content will be written to stdout.
Use --metadata flag to retrieve only metadata instead of the file content.`,
	Example: `  objstore get myfile.txt                        # Download to stdout
  objstore get myfile.txt downloaded.txt         # Download to file
  objstore get logs/2024/app.log -               # Download to stdout explicitly
  objstore get myfile.txt --metadata             # Get metadata only
  objstore get myfile.txt --metadata -o json     # Get metadata as JSON`,
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		metadataOnly, _ := cmd.Flags().GetBool("metadata") //nolint:errcheck // flags are validated by cobra

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		// If --metadata flag is set, return metadata only
		if metadataOnly {
			metadata, err := ctx.GetMetadataCommand(key)
			if err != nil {
				fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
				return err
			}
			fmt.Print(cli.FormatMetadataResult(metadata, cli.OutputFormat(globalConfig.OutputFormat)))
			return nil
		}

		// Otherwise, download the file
		outputPath := ""
		if len(args) > 1 {
			outputPath = args[1]
		}

		if err := ctx.GetCommand(key, outputPath); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		// Only print success message if not writing to stdout
		if outputPath != "" && outputPath != "-" {
			result := &cli.OperationResult{
				Success: true,
				Message: fmt.Sprintf("Successfully downloaded '%s' to '%s'", key, outputPath),
			}
			fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		}
		return nil
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete an object from storage",
	Long:  `Delete an object from the object storage backend.`,
	Example: `  objstore delete myfile.txt                     # Delete a file
  objstore delete logs/2024/app.log              # Delete file with prefix
  objstore delete temp/                          # Delete a specific key (not recursive)`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.DeleteCommand(key); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		result := &cli.OperationResult{
			Success: true,
			Message: fmt.Sprintf("Successfully deleted '%s'", key),
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list [prefix]",
	Short: "List objects in storage",
	Long:  `List all objects in the object storage backend, optionally filtered by prefix.`,
	Example: `  objstore list                                  # List all objects
  objstore list logs/                            # List objects with 'logs/' prefix
  objstore list logs/2024/                       # List objects in logs/2024/
  objstore list -o json                          # List all objects as JSON
  objstore list logs/ -o table                   # List with table format`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		objects, err := ctx.ListCommand(prefix)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		fmt.Print(cli.FormatListResult(objects, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var existsCmd = &cobra.Command{
	Use:   "exists <key>",
	Short: "Check if an object exists",
	Long: `Check if an object exists in the object storage backend.
Returns exit code 0 if the object exists, 1 if it does not.`,
	Example: `  objstore exists myfile.txt                     # Check if file exists
  objstore exists logs/2024/app.log              # Check with prefix
  if objstore exists myfile.txt; then            # Use in shell script
    echo "File exists"
  fi`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		exists, err := ctx.ExistsCommand(key)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		fmt.Print(cli.FormatExistsResult(key, exists, cli.OutputFormat(globalConfig.OutputFormat)))

		// Return non-zero exit code if object doesn't exist
		if !exists {
			os.Exit(1)
		}
		return nil
	},
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show current configuration",
	Long:  `Display the current configuration settings, including backend and output format.`,
	Example: `  objstore config                                # Show current config
  objstore config -o json                        # Show config as JSON
  objstore --backend s3 --backend-bucket mybucket config  # Preview config`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		// For config command, we don't need to create storage backend
		// Just display the configuration
		fmt.Print(cli.DisplayConfig(globalConfig, globalConfig.OutputFormat))
		return nil
	},
}

var archiveCmd = &cobra.Command{
	Use:   "archive <key> <destination-backend>",
	Short: "Archive an object to archival storage",
	Long: `Archive an object to archival storage (local, glacier, azurearchive).
This copies the object to long-term archival storage.

For local archiver, use --destination-path to specify the archive directory.
This allows archiving to a different mount point (e.g., NFS backup server).`,
	Example: `  objstore archive logs/old.log local --destination-path /mnt/backup        # Archive to local backup mount
  objstore archive data.zip local --destination-path /mnt/nfs/backups       # Archive to NFS mount
  objstore archive logs/old.log glacier                                     # Archive to AWS Glacier
  objstore archive backups/2023.tar azurearchive                            # Archive to Azure Archive
  objstore --backend s3 archive old-data.zip glacier                        # From S3 to Glacier`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		destinationBackend := args[1]

		// Get destination-specific settings from flags
		destinationSettings := make(map[string]string)

		if destPath, _ := cmd.Flags().GetString("destination-path"); destPath != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["path"] = destPath
		}
		if destBucket, _ := cmd.Flags().GetString("destination-bucket"); destBucket != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["bucket"] = destBucket
		}
		if destRegion, _ := cmd.Flags().GetString("destination-region"); destRegion != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["region"] = destRegion
		}
		if destKey, _ := cmd.Flags().GetString("destination-key"); destKey != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["access_key_id"] = destKey
		}
		if destSecret, _ := cmd.Flags().GetString("destination-secret"); destSecret != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["secret_access_key"] = destSecret
		}
		if destURL, _ := cmd.Flags().GetString("destination-url"); destURL != "" { //nolint:errcheck // Optional flag, error can be safely ignored
			destinationSettings["endpoint"] = destURL
		}

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.ArchiveCommandWithSettings(key, destinationBackend, destinationSettings); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		result := &cli.OperationResult{
			Success: true,
			Message: fmt.Sprintf("Successfully archived '%s' to %s", key, destinationBackend),
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

// Policy command group
var policyCmd = &cobra.Command{
	Use:   "policy",
	Short: "Manage lifecycle policies",
	Long: `Manage lifecycle policies for automatic object lifecycle management.

Lifecycle policies allow you to automatically delete or archive objects after a specified retention period.`,
	Example: `  objstore policy add cleanup-old-logs logs/ 30 delete    # Delete logs after 30 days
  objstore policy add archive-backups backups/ 90 archive # Archive backups after 90 days
  objstore policy list                                     # List all policies
  objstore policy remove cleanup-old-logs                  # Remove a policy`,
}

var policyAddCmd = &cobra.Command{
	Use:   "add <id> <prefix> <retention-days> <action>",
	Short: "Add a lifecycle policy",
	Long: `Add a lifecycle policy to automatically manage objects.

The policy will apply to all objects matching the specified prefix.
After the retention period (in days), objects will be either deleted or archived.

Actions:
  delete  - Permanently delete objects after retention period
  archive - Move objects to archival storage after retention period`,
	Example: `  objstore policy add cleanup-old-logs logs/ 30 delete           # Delete logs after 30 days
  objstore policy add archive-reports reports/ 365 archive       # Archive reports after 1 year
  objstore policy add temp-cleanup temp/ 1 delete                # Delete temp files after 1 day
  objstore policy add monthly-archive data/monthly/ 90 archive   # Archive monthly data after 90 days`,
	Args: cobra.ExactArgs(4),
	RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]
		prefix := args[1]
		retentionDays := args[2]
		action := args[3]

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.AddPolicyCommand(id, prefix, retentionDays, action); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		result := &cli.OperationResult{
			Success: true,
			Message: fmt.Sprintf("Successfully added policy '%s'", id),
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var policyRemoveCmd = &cobra.Command{
	Use:   "remove <id>",
	Short: "Remove a lifecycle policy",
	Long: `Remove a lifecycle policy by ID.

This stops the policy from being applied to new objects. Existing objects are not affected.`,
	Example: `  objstore policy remove cleanup-old-logs        # Remove policy by ID
  objstore policy remove temp-cleanup            # Remove temp cleanup policy`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]

		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.RemovePolicyCommand(id); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		result := &cli.OperationResult{
			Success: true,
			Message: fmt.Sprintf("Successfully removed policy '%s'", id),
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var policyListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all lifecycle policies",
	Long: `List all configured lifecycle policies.

Shows all active policies with their IDs, prefixes, retention periods, and actions.`,
	Example: `  objstore policy list                           # List all policies
  objstore policy list -o json                   # List policies as JSON
  objstore policy list -o table                  # List policies in table format`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		policies, err := ctx.ListPoliciesCommand()
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		fmt.Print(cli.FormatPoliciesResult(policies, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var policyApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Execute all lifecycle policies",
	Long: `Execute all configured lifecycle policies now.

This scans all objects and applies deletion or archival actions based on configured retention periods.
Use this command in cron jobs for scheduled policy execution.`,
	Example: `  objstore policy apply                          # Apply all policies
  objstore policy apply --server http://localhost:8080  # Apply policies on remote server
  # Cron job example (daily at 2 AM):
  # 0 2 * * * /usr/local/bin/objstore policy apply`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		if err := ctx.ApplyPoliciesCommand(); err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		result := &cli.OperationResult{
			Success: true,
			Message: "Successfully applied all lifecycle policies",
		}
		fmt.Print(cli.FormatOperationResult(result, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check health status",
	Long: `Check the health status of the object storage backend.

Returns the backend status, version, and configuration information.`,
	Example: `  objstore health                                # Check health status
  objstore health -o json                        # Get health status as JSON
  objstore --backend s3 health                   # Check S3 backend health`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, err := cli.NewCommandContext(globalConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}
		defer func() { _ = ctx.Close() }()

		health, err := ctx.HealthCommand()
		if err != nil {
			fmt.Fprintln(os.Stderr, cli.FormatError(err, cli.OutputFormat(globalConfig.OutputFormat)))
			return err
		}

		fmt.Print(cli.FormatHealthResult(health, cli.OutputFormat(globalConfig.OutputFormat)))
		return nil
	},
}

func init() {
	// Set custom usage template to always show examples (even on errors)
	cobra.AddTemplateFunc("hasExamples", func(cmd *cobra.Command) bool {
		return len(cmd.Example) > 0
	})

	usageTemplate := `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

Available Commands:{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

Additional Commands:{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

	// Apply template to all commands
	rootCmd.SetUsageTemplate(usageTemplate)

	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.objstore.yaml)")
	rootCmd.PersistentFlags().String("server", "", "server URL for remote operations (e.g., http://localhost:8080)")
	rootCmd.PersistentFlags().String("server-protocol", "rest", "server protocol: rest, grpc, or quic")
	rootCmd.PersistentFlags().String("backend", "local", "storage backend (local, s3, minio, gcs, azure)")
	rootCmd.PersistentFlags().String("backend-path", "./storage", "path for local backend")
	rootCmd.PersistentFlags().String("backend-bucket", "", "bucket name for cloud backends")
	rootCmd.PersistentFlags().String("backend-region", "", "region for cloud backends")
	rootCmd.PersistentFlags().String("backend-key", "", "access key for cloud backends")
	rootCmd.PersistentFlags().String("backend-secret", "", "secret key for cloud backends")
	rootCmd.PersistentFlags().String("backend-url", "", "custom endpoint URL for cloud backends")
	rootCmd.PersistentFlags().StringP("output-format", "o", "text", "output format (text, json, table)")

	// get command flags
	getCmd.Flags().Bool("metadata", false, "retrieve only metadata (not file content)")

	// put command flags for metadata
	putCmd.Flags().String("content-type", "", "content type for the object")
	putCmd.Flags().String("content-encoding", "", "content encoding for the object")
	putCmd.Flags().StringToString("custom", map[string]string{}, "custom metadata fields (key=value pairs)")

	// archive command flags for destination settings
	archiveCmd.Flags().String("destination-path", "", "path for local archiver (e.g., /mnt/backup)")
	archiveCmd.Flags().String("destination-bucket", "", "bucket name for cloud archivers")
	archiveCmd.Flags().String("destination-region", "", "region for cloud archivers")
	archiveCmd.Flags().String("destination-key", "", "access key for cloud archivers")
	archiveCmd.Flags().String("destination-secret", "", "secret key for cloud archivers")
	archiveCmd.Flags().String("destination-url", "", "custom endpoint URL for cloud archivers")

	// Add policy subcommands
	policyCmd.AddCommand(policyAddCmd)
	policyCmd.AddCommand(policyListCmd)
	policyCmd.AddCommand(policyRemoveCmd)
	policyCmd.AddCommand(policyApplyCmd)

	// Add commands to root
	rootCmd.AddCommand(putCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(existsCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(archiveCmd)
	rootCmd.AddCommand(policyCmd)
	rootCmd.AddCommand(healthCmd)

	// Apply usage template to all commands to ensure examples always show
	for _, cmd := range rootCmd.Commands() {
		cmd.SetUsageTemplate(usageTemplate)
		// Also apply to subcommands
		for _, subCmd := range cmd.Commands() {
			subCmd.SetUsageTemplate(usageTemplate)
		}
	}
}
