package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/cmd/bolted-tui/filepicker"
	"github.com/urfave/cli/v2"
)

type model struct {
	filepicker   filepicker.Model
	selectedFile string
	quitting     bool
	err          error
}

type clearErrorMsg struct{}

func clearErrorAfter(t time.Duration) tea.Cmd {
	return tea.Tick(t, func(_ time.Time) tea.Msg {
		return clearErrorMsg{}
	})
}

func (m model) Init() tea.Cmd {
	return m.filepicker.Init()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		}
	case clearErrorMsg:
		m.err = nil
	}

	var cmd tea.Cmd
	m.filepicker, cmd = m.filepicker.Update(msg)

	// Did the user select a file?
	didSelect, path := m.filepicker.DidSelectFile(msg)
	if didSelect {
		// Get the path of the selected file.
		m.selectedFile = path
	}

	// Did the user select a disabled file?
	// This is only necessary to display an error to the user.
	didSelectDisabled, path := m.filepicker.DidSelectDisabledFile(msg)
	if didSelectDisabled {
		// Let's clear the selectedFile and display an error.
		m.err = errors.New(path + " is not valid.")
		m.selectedFile = ""
		return m, tea.Batch(cmd, clearErrorAfter(2*time.Second))
	}

	return m, cmd
}

func (m model) View() string {
	if m.quitting {
		return ""
	}
	var s strings.Builder
	s.WriteString("\n  ")
	if m.err != nil {
		s.WriteString(m.filepicker.Styles.DisabledFile.Render(m.err.Error()))
	} else if m.selectedFile == "" {
		s.WriteString(m.filepicker.CurrentDirectory)
	} else {
		s.WriteString("Selected file: " + m.filepicker.Styles.Selected.Render(m.selectedFile))
	}
	s.WriteString("\n\n" + m.filepicker.View() + "\n")
	return s.String()
}

func main() {
	app := &cli.App{
		Action: func(c *cli.Context) error {

			if c.NArg() != 1 {
				return fmt.Errorf("db file must be provided")
			}

			db, err := bolted.Open(c.Args().First(), 0700, bolted.Options{})
			if err != nil {
				return fmt.Errorf("could not open db: %w", err)
			}

			// return fs.WalkDir(boltedfs.New(db), "", func(path string, d fs.DirEntry, err error) error {
			// 	fmt.Println(path)
			// 	return nil
			// })

			fp := filepicker.New(db)
			fp.AllowedTypes = []string{".mod", ".sum", ".go", ".txt", ".md"}
			fp.CurrentDirectory = ""

			m := &model{
				filepicker: fp,
			}
			tp := tea.NewProgram(m, tea.WithOutput(os.Stderr))
			tm, err := tp.Run()
			if err != nil {
				return fmt.Errorf("teabubble error: %w", err)
			}
			mm := tm.(*model)
			fmt.Println("\n  You selected: " + m.filepicker.Styles.Selected.Render(mm.selectedFile) + "\n")
			return nil
		},
	}
	app.RunAndExitOnError()
}
