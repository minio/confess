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
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
	"github.com/minio/confess/tests"
)

const (
	padding                    = 1
	tick                       = "✔"
	useHighPerformanceRenderer = true
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
	ready           bool
	viewport        viewport.Model
	maxHeight       int
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
		var cmd tea.Cmd
		var cmds []tea.Cmd
		if !m.ready {
			m.viewport = viewport.New(msg.Width, lipgloss.Height(m.getContent()))
			m.viewport.HighPerformanceRendering = useHighPerformanceRenderer
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
		}
		m.maxHeight = msg.Height - lipgloss.Height(m.tableView())
		if useHighPerformanceRenderer {
			cmds = append(cmds, viewport.Sync(m.viewport))
		}
		m.viewport, cmd = m.viewport.Update(msg)
		cmds = append(cmds, cmd)
		return m, tea.Batch(cmds...)
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
		var cmd, vCmd tea.Cmd
		var cmds []tea.Cmd
		m.table, cmd = m.table.Update(msg)
		if msg.err != nil {
			m.errLogs = append(m.errLogs, msg.err.Error())
		}
		cmds = append(cmds, cmd)
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
		if m.ready {
			if contentHeight := lipgloss.Height(m.getContent()); contentHeight < m.maxHeight {
				m.viewport.Height = contentHeight
			}
			m.viewport.SetContent(m.getContent())
			m.viewport, vCmd = m.viewport.Update(msg)
			cmds = append(cmds, []tea.Cmd{vCmd, viewport.Sync(m.viewport)}...)
		}
		return m, tea.Batch(cmds...)
	default:
		var sCmd, vCmd tea.Cmd
		var cmds []tea.Cmd
		m.viewport, vCmd = m.viewport.Update(msg)
		m.spinner, sCmd = m.spinner.Update(msg)
		cmds = append(cmds, []tea.Cmd{sCmd, vCmd}...)
		return m, tea.Batch(cmds...)
	}
}

func (m progressModel) padding() string {
	return strings.Repeat(" ", padding)
}

func (m progressModel) getContent() (str string) {
	pad := m.padding()
	for i := range m.errLogs {
		str += "\n" + pad + fmt.Sprintf(color.HiRedString("%s", m.errLogs[i]))
		if i == len(m.errLogs)-1 {
			str += "\n"
		}
	}
	for i := range m.logs {
		str += "\n" + pad + fmt.Sprintf(color.HiYellowString("%s", m.logs[i]))
	}
	return
}

func (m progressModel) tableView() (str string) {
	totalAPIs, failedAPIs := m.stats.APIInfo()
	totalTests, failedTests := m.stats.TestInfo()
	totalCount := totalAPIs + totalTests
	failedCount := failedAPIs + failedTests
	duration := time.Since(m.startTime)
	rows := []table.Row{
		{
			color.HiWhiteString("%d", totalCount),
			color.HiWhiteString("%d", totalCount-failedCount),
			color.HiRedString("%d", failedCount),
			color.HiWhiteString(duration.Round(time.Second).String()),
		},
	}
	m.table.SetRows(rows)
	return m.padding() + "\n\n" + baseStyle.Render(m.table.View()) + "\n\n"
}

func (m progressModel) View() string {
	if !m.ready {
		return ""
	}
	str := m.tableView()
	if m.waitForCleanup {
		if m.done {
			str += m.padding() + fmt.Sprintf("%s %s\n", color.HiYellowString("Cleaned up test suite"), m.spinner.Style.Render(tick))
		} else {
			str += m.padding() + fmt.Sprintf("%s %s\n", color.HiYellowString("Cleaning up test suite. Please wait"), m.spinner.View())
		}
	}
	return m.viewport.View() + str
}
