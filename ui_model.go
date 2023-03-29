// Copyright (c) 2023 MinIO, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
	"github.com/minio/confess/stats"
)

const (
	padding = 1
	tick    = "✔"
	cross   = "✗"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type progressLog struct {
	log  string
	done bool
	err  error
}

type progressNotification struct {
	log          string
	progressLogs []progressLog
	done         bool
	err          error
}

type progressModel struct {
	spinner      spinner.Model
	stats        *stats.APIStats
	table        table.Model
	progressLogs []progressLog
	logs         []string
	errLogs      []string
	done         bool
}

func newProgressModel(stats *stats.APIStats) *progressModel {
	progressM := &progressModel{}
	progressM.spinner = spinner.New()
	progressM.spinner.Spinner = spinner.Meter
	progressM.spinner.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#E7B10A"))
	progressM.stats = stats

	columns := []table.Column{
		{Title: "API", Width: 10},
		{Title: "TOTAL", Width: 10},
		{Title: "SUCCESS", Width: 10},
	}
	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(false),
		table.WithHeight(4),
	)
	s := table.Styles{
		Header: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#576CBC")).Padding(0, 1),
		Cell:   lipgloss.NewStyle().Padding(0, 1),
	}
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	t.SetStyles(s)

	progressM.table = t

	return progressM
}

func finalPause() tea.Cmd {
	return tea.Tick(time.Millisecond*750, func(_ time.Time) tea.Msg {
		return nil
	})
}

func (m progressModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		}
		return m, nil

	case progressNotification:
		if msg.log != "" {
			if m.logs == nil {
				m.logs = []string{msg.log}
			} else {
				m.logs = append(m.logs, msg.log)
			}
		}
		if len(msg.progressLogs) > 0 {
			m.progressLogs = msg.progressLogs
		}
		if msg.err != nil {
			m.errLogs = append(m.errLogs, msg.err.Error())
		}
		var cmd tea.Cmd
		m.table, cmd = m.table.Update(msg)
		cmds := []tea.Cmd{
			cmd,
		}
		if msg.done {
			m.done = msg.done
			cmds = append(cmds, tea.Sequence(finalPause(), tea.Quit))
		}

		return m, tea.Batch(cmds...)

	// FrameMsg is sent when the progress bar wants to animate itself
	case progress.FrameMsg:
		return m, nil

	default:
		var sCmd, tCmd tea.Cmd
		m.spinner, sCmd = m.spinner.Update(msg)
		m.table, tCmd = m.table.Update(msg)
		cmds := []tea.Cmd{
			sCmd,
			tCmd,
		}
		return m, tea.Batch(cmds...)
	}
}

func (m progressModel) View() (str string) {

	rows := []table.Row{
		{"PUT", strconv.Itoa(m.stats.Puts.TotalCount), strconv.Itoa(m.stats.Puts.SuccessCount)},
		{"HEAD", strconv.Itoa(m.stats.Heads.TotalCount), strconv.Itoa(m.stats.Heads.SuccessCount)},
		{"LIST", strconv.Itoa(m.stats.Lists.TotalCount), strconv.Itoa(m.stats.Lists.SuccessCount)},
		{"DELETE", strconv.Itoa(m.stats.Deletes.TotalCount), strconv.Itoa(m.stats.Deletes.SuccessCount)},
	}
	m.table.SetRows(rows)

	str += "\n" + baseStyle.Render(m.table.View()) + "\n\n"

	pad := strings.Repeat(" ", padding)
	for i := range m.progressLogs {
		if m.progressLogs[i].done {
			symbol := tick
			if m.progressLogs[i].err != nil {
				symbol = cross
			}
			str += pad + fmt.Sprintf("%s %s\n", color.HiYellowString(m.progressLogs[i].log), m.spinner.Style.Render(symbol))
		} else {
			str += pad + fmt.Sprintf("%s %s\n", color.HiYellowString(m.progressLogs[i].log), m.spinner.View())
		}
		if i == len(m.progressLogs)-1 {
			str += "\n"
		}
	}
	for i := range m.logs {
		str += pad + fmt.Sprintln(color.HiYellowString("%s", m.logs[i]))
		if i == len(m.logs)-1 {
			str += "\n"
		}
	}
	for i := range m.errLogs {
		str += pad + fmt.Sprintln(color.HiRedString("%s", m.errLogs[i]))
		if i == len(m.errLogs)-1 {
			str += "\n"
		}
	}
	return str + pad
}
