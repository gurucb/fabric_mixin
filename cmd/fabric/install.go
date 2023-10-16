package main

import (
	"github.com/getporter/fabric/pkg/fabric"
	"github.com/spf13/cobra"
)

var (
	commandFile string
)

func buildInstallCommand(m *fabric.Mixin) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Execute the install functionality of this mixin",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			//Do something here if needed
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return m.Execute(cmd.Context())
		},
	}
	return cmd
}
