package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	ownerSuffix        = "_owner"
	schemaAdminSuffix  = "_schadm"
	roSuffix           = "_ro"
	rwSuffix           = "_rw"
	groupSuffix        = "_grp"
	userSuffix         = "_usr"
	envVarOutCredsFile = "PG_TENANT_SETUP_OUTPUT_CREDENTIALS_FILE"
	envVarOutSQLFile   = "PG_TENANT_SETUP_OUTPUT_SQL_FILE"
	envVarHaltOnError  = "PG_TENANT_SETUP_HALT_ON_ERROR"
	outFileMode        = 0600
)

type PGConnExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type PasswordConfig struct {
	Length         int
	UseLetters     bool
	UseSpecial     bool
	UseNum         bool
	ExcludeSpecial string `default:"@/"`
}

type ConnectDBConfig struct {
	DBName   string
	RoleName string
}

type UserCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SchemaGroups struct {
	Admin     string `json:"admin"`
	ReadWrite string `json:"readwrite"`
	ReadOnly  string `json:"readonly"`
}

type SchemaUsers struct {
	Admin     UserCredentials `json:"admin"`
	ReadWrite UserCredentials `json:"readwrite"`
	ReadOnly  UserCredentials `json:"readonly"`
}
