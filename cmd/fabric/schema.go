package main

import (
	"github.com/getporter/fabric/pkg/fabric"
	"github.com/spf13/cobra"
)

func buildSchemaCommand(m *fabric.Mixin) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Print the json schema for the mixin",
		Run: func(cmd *cobra.Command, args []string) {
			m.PrintSchema()
		},
	}
	return cmd
}
