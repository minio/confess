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

package utils

import (
	"os"
)

// Logger implements the Log interface.
type Logger interface {
	Log(message string) error
	V(level int) Logger
}

// FileLogger logs the messages to a file.
type FileLogger struct {
	file      *os.File
	verbosity int
}

// Log writes the message to the file.
func (l *FileLogger) Log(message string) error {
	_, err := l.file.WriteString(message)
	return err
}

// New returns the FileLogger instance.
func NewLogger(file *os.File, verbosity int) *FileLogger {
	return &FileLogger{
		file:      file,
		verbosity: verbosity,
	}
}

// V returns the FileLogger if the verbosity level is less than
// the configured level. Else, returns a no-op logger.
func (l *FileLogger) V(v int) Logger {
	if v <= l.verbosity {
		return l
	}
	return NoOpLogger{}
}

// NoOpLogger is used for a nil logger.
type NoOpLogger struct{}

// Log does a No-Op.
func (l NoOpLogger) Log(message string) error {
	return nil
}

// V returns the FileLogger if the verbosity level is less than
// the configured level. Else, returns a no-op logger.
func (l NoOpLogger) V(v int) Logger {
	return NoOpLogger{}
}
