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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
	"github.com/minio/confess/tests"
)

const (
	padding = 1
	tick    = "✔"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("FCFFE7"))

type progressModel struct {
	stats           *tests.Stats
	errLogs         []string
	logs            []string
	done            bool
	startTime       time.Time
	table           table.Model
	spinner         spinner.Model
	cancelParentCtx context.CancelFunc
	waitForCleanup  bool
}

func newProgressModel(stats *tests.Stats, parentCtxCancel context.CancelFunc) *progressModel {
	progressM := &progressModel{}
	progressM.stats = stats
	progressM.startTime = time.Now()
	progressM.cancelParentCtx = parentCtxCancel

	columns := []table.Column{
		{Title: "Total Operations", Width: 20},
		{Title: "Succeeded", Width: 20},
		{Title: "Failed", Width: 20},
		{Title: "Duration", Width: 20},
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

	progressM.spinner = spinner.New()
	progressM.spinner.Spinner = spinner.Points
	progressM.spinner.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#F7971E"))

	return progressM
}

type notification struct {
	waitForCleanup bool
	log            string
	done           bool
	err            error
}

type TickMsg time.Time

func tickEvery() tea.Cmd {
	return tea.Every(time.Millisecond, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

func finalPause() tea.Cmd {
	return tea.Tick(time.Second*1, func(_ time.Time) tea.Msg {
		return nil
	})
}

func (m progressModel) initSpinner() tea.Cmd {
	return m.spinner.Tick
}

func (m progressModel) Init() tea.Cmd {
	return tea.Batch([]tea.Cmd{tickEvery(), m.initSpinner()}...)
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		return m, nil
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.waitForCleanup = true
			m.cancelParentCtx()
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
		if msg.log != "" {
			m.logs = append(m.logs, msg.log)
		}
		if msg.waitForCleanup {
			m.waitForCleanup = true
		}
		if msg.done {
			m.done = msg.done
			return m, tea.Sequence(finalPause(), tea.Quit)
		}
		return m, cmd
	default:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
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

	for i := range m.logs {
		str += pad + fmt.Sprintln(color.HiYellowString("%s", m.logs[i]))
		if i == len(m.logs)-1 {
			str += "\n"
		}
	}

	if m.waitForCleanup {
		if m.done {
			str += pad + fmt.Sprintf("%s %s\n", color.HiYellowString("Cleaned up test suite"), m.spinner.Style.Render(tick))
		} else {
			str += pad + fmt.Sprintf("%s %s\n", color.HiYellowString("Cleaning up test suite. Please wait"), m.spinner.View())
		}
	}

	return str + pad
}
