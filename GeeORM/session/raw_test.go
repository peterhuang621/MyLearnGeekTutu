package session

import (
	"GeeORM/dialect"
	"database/sql"
)

var (
	TestDB      *sql.DB
	TestDial, _ = dialect.GetDialect("sqlite3")
)

func NewSession() *Session {
	return New(TestDB, TestDial)
}
