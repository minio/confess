// Copyright (c) 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/minio/cli"
)

var (
	baseStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("0")).
			Width(106).
			Bold(false)
	errMsgStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("204")).
			Bold(true).
			Align(lipgloss.Left).
			Height(5).
			Width(50)
	nodeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#ffffff")).
			Bold(true)
	whiteStyle = lipgloss.NewStyle().
			Bold(false).
			Foreground(lipgloss.Color("#ffffff"))
	subtle    = lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#383838"}
	warnColor = lipgloss.AdaptiveColor{Light: "#bf4364", Dark: "#e31441"}
	special   = lipgloss.AdaptiveColor{Light: "#22E32F", Dark: "#07E316"}

	advisory = lipgloss.NewStyle().Foreground(special).Render
	warn     = lipgloss.NewStyle().Foreground(warnColor).Render

	// Status Bar.

	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.AdaptiveColor{Light: "#343433", Dark: "#C1C6B2"}).
			Background(lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#353533"})

	statusStyle = lipgloss.NewStyle().
			Inherit(statusBarStyle).
			Foreground(lipgloss.Color("37")).
			Background(lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#353533"}).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Inherit(statusStyle).
			Foreground(lipgloss.Color("118")).
			Background(lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#353533"}).
			Bold(true)
	statusTextStyle = lipgloss.NewStyle().
			Inherit(statusStyle).
			Foreground(lipgloss.AdaptiveColor{Light: "#343433", Dark: "#C1C6B2"}).
			Background(lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#353533"}).
			Bold(false)

	failedStyle = lipgloss.NewStyle().
			Inherit(statusStyle).
			Foreground(lipgloss.Color("190")).
			Background(lipgloss.AdaptiveColor{Light: "#D9DCCF", Dark: "#353533"}).
			Bold(true)
)

func getHeader(ctx *cli.Context) string {
	var s strings.Builder
	s.WriteString("confess " + getVersion() + " ")
	flags := ctx.GlobalFlagNames()
	for idx, flag := range flags {
		if !ctx.IsSet(flag) {
			continue
		}
		switch {
		case ctx.Bool(flag):
			s.WriteString(fmt.Sprintf("%s=%t", flag, ctx.Bool(flag)))
		case ctx.String(flag) != "":
			val := ctx.String(flag)
			if flag == "secret-key" {
				val = "*REDACTED*"
			}
			s.WriteString(fmt.Sprintf("%s=%s", flag, val))
		}
		if idx != len(flags)-1 {
			s.WriteString(" ")
		}
	}
	s.WriteString("\n")
	return s.String()
}
