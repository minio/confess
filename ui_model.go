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
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
	"github.com/minio/confess/tests"
)

const padding = 1

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("FCFFE7"))

type progressModel struct {
	stats     *tests.Stats
	errLogs   []string
	done      bool
	startTime time.Time
	table     table.Model
}

func newProgressModel(stats *tests.Stats) *progressModel {
	progressM := &progressModel{}
	progressM.stats = stats
	progressM.startTime = time.Now()

	columns := []table.Column{
		{Title: "Total Operations", Width: 20},
		{Title: "Succeeded", Width: 10},
		{Title: "Failed", Width: 10},
		{Title: "Duration", Width: 10},
	}
	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(false),
		table.WithHeight(1),
	)
	s := table.Styles{
		Header: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#F99417")).Padding(0, 1),
		Cell:   lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#04B575")).Padding(0, 1),
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

type notification struct {
	log  string
	done bool
	err  error
}

type TickMsg time.Time

func tickEvery() tea.Cmd {
	return tea.Every(time.Millisecond, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

func finalPause() tea.Cmd {
	return tea.Tick(time.Millisecond*750, func(_ time.Time) tea.Msg {
		return nil
	})
}

func (m progressModel) Init() tea.Cmd {
	return tickEvery()
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
	case TickMsg:
		var cmd tea.Cmd
		var cmds []tea.Cmd
		m.table, cmd = m.table.Update(msg)
		cmds = append(cmds, cmd, tickEvery())
		return m, tea.Batch(cmds...)
	case notification:
		var cmd tea.Cmd
		m.table, cmd = m.table.Update(msg)
		if msg.err != nil {
			m.errLogs = append(m.errLogs, msg.err.Error())
		}
		if msg.done {
			m.done = msg.done
			return m, tea.Sequence(finalPause(), tea.Quit)
		}
		return m, cmd
	default:
		return m, nil
	}
}

func (m progressModel) View() (str string) {
	totalCount, successCount := m.stats.Info()
	duration := time.Since(m.startTime)
	rows := []table.Row{
		{
			color.HiWhiteString("%d", totalCount),
			color.HiWhiteString("%d", successCount),
			color.HiRedString("%d", totalCount-successCount),
			color.HiWhiteString(duration.Round(time.Second).String()),
		},
	}
	m.table.SetRows(rows)
	pad := strings.Repeat(" ", padding)
	str += pad + "\n" + baseStyle.Render(m.table.View()) + "\n\n"

	for i := range m.errLogs {
		str += pad + fmt.Sprintln(color.HiRedString("%s", m.errLogs[i]))
		if i == len(m.errLogs)-1 {
			str += "\n"
		}
	}

	return str + pad
}
