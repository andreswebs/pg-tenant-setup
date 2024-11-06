package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	db *pgxpool.Pool
}

var (
	pgInstance *Postgres
	pgOnce     sync.Once
)

func Connect(ctx context.Context, connString string) (*Postgres, error) {
	pgOnce.Do(func() {
		db, err := pgxpool.New(ctx, connString)
		if err != nil {
			err = fmt.Errorf("unable to create connection pool: %w", err)
			return
		}

		pgInstance = &Postgres{db}
	})

	outSQLFile := os.Getenv(envVarOutSQLFile)
	if outSQLFile != "" {
		truncateFile(outSQLFile)
	}

	return pgInstance, nil
}

func (pg *Postgres) ConnectDB(ctx context.Context, connConfig ConnectDBConfig) (pool *pgxpool.Pool, err error) {
	config := pg.db.Config().Copy()
	if connConfig.DBName != "" {
		config.ConnConfig.Database = connConfig.DBName
	}

	config.BeforeConnect = func(ctx context.Context, connConfig *pgx.ConnConfig) (err error) {
		outSQLFile := os.Getenv(envVarOutSQLFile)
		if outSQLFile != "" {
			appendToFile(outSQLFile, fmt.Sprintf("-- switching to database %s\n", connConfig.Database))
		}
		return
	}

	if connConfig.RoleName != "" {
		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
			setRole := fmt.Sprintf("SET ROLE %s;", connConfig.RoleName)
			_, err = pg.RunExec(conn, ctx, setRole)
			return
		}

		config.BeforeClose = func(conn *pgx.Conn) {
			resetRole := fmt.Sprintf("RESET ROLE;")
			pg.RunExec(conn, ctx, resetRole)
		}
	}

	config.MaxConns = 1
	config.MinConns = 1

	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		err = fmt.Errorf("unable to create connection pool: %w", err)
		return
	}

	return
}

func (pg *Postgres) RunExec(x PGConnExecutor, ctx context.Context, sql string, arguments ...any) (tag pgconn.CommandTag, err error) {
	tag, err = x.Exec(ctx, sql, arguments...)
	if err != nil {
		err = fmt.Errorf("%w\nwith sql:\n%s", err, sql)
		haltOnError := os.Getenv(envVarHaltOnError)
		if haltOnError != "" {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	}

	outSQLFile := os.Getenv(envVarOutSQLFile)
	if outSQLFile != "" {
		appendToFile(outSQLFile, fmt.Sprintf("%s\n", sql))
	}

	return
}

func (pg *Postgres) Ping(ctx context.Context) error {
	return pg.db.Ping(ctx)
}

func (pg *Postgres) Close() {
	pg.db.Close()
}

func (pg *Postgres) NewTenantSchemaGroups(ctx context.Context, dbName string, schemaName string) SchemaGroups {

	schemaGroups := tenantSchemaGroupNames(dbName, schemaName)

	dropOwnedByAdmin := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.Admin, schemaGroups.Admin)
	dropAdmin := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.Admin)
	createAdmin := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.Admin)

	dropOwnedByRW := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.ReadWrite, schemaGroups.ReadWrite)
	dropRW := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadWrite)
	createRW := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadWrite)

	dropOwnedByRO := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.ReadOnly, schemaGroups.ReadOnly)
	dropRO := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadOnly)
	createRO := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadOnly)

	pg.RunExec(pg.db, ctx, dropOwnedByAdmin)
	pg.RunExec(pg.db, ctx, dropAdmin)
	pg.RunExec(pg.db, ctx, createAdmin)

	pg.RunExec(pg.db, ctx, dropOwnedByRW)
	pg.RunExec(pg.db, ctx, dropRW)
	pg.RunExec(pg.db, ctx, createRW)

	pg.RunExec(pg.db, ctx, dropOwnedByRO)
	pg.RunExec(pg.db, ctx, dropRO)
	pg.RunExec(pg.db, ctx, createRO)

	return schemaGroups
}

func (pg *Postgres) NewTenantSchemaUsers(ctx context.Context, dbName string, schemaName string) SchemaUsers {

	schemaGroups := tenantSchemaGroupNames(dbName, schemaName)
	schemaUsers := newTenantSchemaUserCredentials(dbName, schemaName)

	dropOwnedByAdmin := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.Admin.Username, schemaUsers.Admin.Username)
	dropAdmin := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.Admin.Username)
	createAdmin := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.Admin.Username, schemaUsers.Admin.Password)
	grantAdmin := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.Admin, schemaUsers.Admin.Username)

	dropOwnedByRW := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Username)
	dropRW := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadWrite.Username)
	createRW := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Password)
	grantRW := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadWrite, schemaUsers.ReadWrite.Username)

	dropOwnedByRO := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Username)
	dropRO := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadOnly.Username)
	createRO := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Password)
	grantRO := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadOnly, schemaUsers.ReadOnly.Username)

	pg.RunExec(pg.db, ctx, dropOwnedByAdmin)
	pg.RunExec(pg.db, ctx, dropAdmin)
	pg.RunExec(pg.db, ctx, createAdmin)
	pg.RunExec(pg.db, ctx, grantAdmin)

	pg.RunExec(pg.db, ctx, dropOwnedByRW)
	pg.RunExec(pg.db, ctx, dropRW)
	pg.RunExec(pg.db, ctx, createRW)
	pg.RunExec(pg.db, ctx, grantRW)

	pg.RunExec(pg.db, ctx, dropOwnedByRO)
	pg.RunExec(pg.db, ctx, dropRO)
	pg.RunExec(pg.db, ctx, createRO)
	pg.RunExec(pg.db, ctx, grantRO)

	return schemaUsers
}

func (pg *Postgres) NewTenantDB(ctx context.Context, dbName string) (err error) {
	ownerRole := tenantOwnerName(dbName)

	var currentRole string
	err = pg.db.QueryRow(ctx, "SELECT current_role").Scan(&currentRole)
	if err != nil {
		return
	}

	// begin definitions

	alterDBToCurrent := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, currentRole)
	dropOwned := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", ownerRole, ownerRole)
	dropOwner := fmt.Sprintf("DROP ROLE IF EXISTS %s;", ownerRole)
	dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName)
	createOwner := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", ownerRole)
	createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
	alterDB := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, ownerRole)

	// revoke all privileges from PUBLIC

	revokeDBPublic := fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC;", dbName)
	revokeSchemaPublic := fmt.Sprintf("REVOKE CREATE ON SCHEMA public FROM PUBLIC;")

	// begin executions

	pg.RunExec(pg.db, ctx, alterDBToCurrent)
	pg.RunExec(pg.db, ctx, dropDB)
	pg.RunExec(pg.db, ctx, dropOwned)
	pg.RunExec(pg.db, ctx, dropOwner)

	_, err = pg.RunExec(pg.db, ctx, createOwner)
	if err != nil {
		err = fmt.Errorf("unable to create owner role: %w", err)
		return
	}

	_, err = pg.RunExec(pg.db, ctx, createDB)
	if err != nil {
		err = fmt.Errorf("unable to create database: %w", err)
		pg.RunExec(pg.db, ctx, dropOwner)
		return
	}

	_, err = pg.RunExec(pg.db, ctx, alterDB)
	if err != nil {
		err = fmt.Errorf("unable to set database owner: %w", err)
		pg.RunExec(pg.db, ctx, dropDB)
		pg.RunExec(pg.db, ctx, dropOwned)
		pg.RunExec(pg.db, ctx, dropOwner)
		return
	}

	// execute revoke all privileges from PUBLIC

	pg.RunExec(pg.db, ctx, revokeDBPublic)

	tmpPool, err := pg.ConnectDB(ctx, ConnectDBConfig{DBName: dbName})
	if err != nil {
		return
	}

	defer tmpPool.Close()

	conn, err := tmpPool.Acquire(ctx)
	if err != nil {
		err = fmt.Errorf("unable to acquire connection: %w", err)
		return
	}

	defer conn.Release()

	pg.RunExec(conn, ctx, revokeSchemaPublic)

	return
}

func (pg *Postgres) NewTenantSchema(ctx context.Context, schemaName string, connConfig ConnectDBConfig) (err error) {

	if connConfig.DBName == "" {
		err = fmt.Errorf("missing database name: %w", err)
		return
	}

	dbName := connConfig.DBName
	ownerRole := tenantOwnerName(dbName)

	if connConfig.RoleName == "" {
		connConfig.RoleName = ownerRole
	}

	tenantGroups := pg.NewTenantSchemaGroups(ctx, dbName, schemaName)

	// grant basic privileges
	grantDBAccess := fmt.Sprintf(
		"GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s;",
		dbName, fmt.Sprintf("%s, %s, %s", tenantGroups.Admin, tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)
	pg.RunExec(pg.db, ctx, grantDBAccess)

	tmpPool, err := pg.ConnectDB(ctx, connConfig)
	if err != nil {
		err = fmt.Errorf("unable to connect to database: %w", err)
		return
	}

	defer tmpPool.Close()

	conn, err := tmpPool.Acquire(ctx)
	if err != nil {
		err = fmt.Errorf("unable to acquire connection: %w", err)
		return
	}

	defer conn.Release()

	// begin definitions

	dropSchema := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schemaName)
	createSchema := fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
	revokeCreate := fmt.Sprintf("REVOKE CREATE ON SCHEMA %s FROM PUBLIC;", schemaName)

	// admin privileges

	grantSchemaAdminCreate := fmt.Sprintf("GRANT USAGE, CREATE ON SCHEMA %s TO %s;", schemaName, tenantGroups.Admin)
	grantSchemaAdminTables := fmt.Sprintf("GRANT ALL ON ALL TABLES IN SCHEMA %s TO %s;", schemaName, tenantGroups.Admin)
	grantSchemaAdminSequences := fmt.Sprintf("GRANT ALL ON ALL SEQUENCES IN SCHEMA %s TO %s;", schemaName, tenantGroups.Admin)

	// basic privileges

	grantSchemaUsage := fmt.Sprintf(
		"GRANT USAGE ON SCHEMA %s TO %s;",
		schemaName, fmt.Sprintf("%s, %s", tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)

	grantTablesRead := fmt.Sprintf(
		"GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;",
		schemaName, fmt.Sprintf("%s, %s", tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)

	grantSequencesRead := fmt.Sprintf(
		"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;",
		schemaName, fmt.Sprintf("%s, %s", tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)

	// default privileges

	// partial cmd
	defaultAlter := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s", schemaName)

	grantDefaultSequencesRead := fmt.Sprintf(
		"%s GRANT USAGE, SELECT ON SEQUENCES TO %s;",
		defaultAlter, fmt.Sprintf("%s, %s", tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)

	grantDefaultSequencesWrite := fmt.Sprintf(
		"%s GRANT UPDATE ON SEQUENCES TO %s;",
		defaultAlter, tenantGroups.ReadWrite,
	)

	grantDefaultTablesRead := fmt.Sprintf(
		"%s GRANT SELECT ON TABLES TO %s;",
		defaultAlter, tenantGroups.ReadOnly,
	)

	grantDefaultTablesReadWrite := fmt.Sprintf(
		"%s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s;",
		defaultAlter, tenantGroups.ReadWrite,
	)

	pg.RunExec(conn, ctx, dropSchema)

	_, err = pg.RunExec(conn, ctx, createSchema)
	if err != nil {
		err = fmt.Errorf("unable to create schema: %w", err)
		return
	}

	// begin executions

	pg.RunExec(conn, ctx, revokeCreate)
	pg.RunExec(conn, ctx, grantSchemaAdminCreate)
	pg.RunExec(conn, ctx, grantSchemaAdminTables)
	pg.RunExec(conn, ctx, grantSchemaAdminSequences)

	pg.RunExec(conn, ctx, grantSchemaUsage)
	pg.RunExec(conn, ctx, grantTablesRead)
	pg.RunExec(conn, ctx, grantSequencesRead)

	pg.RunExec(conn, ctx, grantDefaultSequencesRead)
	pg.RunExec(conn, ctx, grantDefaultSequencesWrite)
	pg.RunExec(conn, ctx, grantDefaultTablesRead)
	pg.RunExec(conn, ctx, grantDefaultTablesReadWrite)

	tenantUsers := pg.NewTenantSchemaUsers(ctx, dbName, schemaName)

	outCredsFile := os.Getenv(envVarOutCredsFile)

	if outCredsFile != "" {
		tenantUsersData, err := json.Marshal(tenantUsers)
		if err != nil {
			return fmt.Errorf("unable to marshal tenant users data: %w", err)
		}

		err = os.WriteFile(outCredsFile, tenantUsersData, outFileMode)
		if err != nil {
			return fmt.Errorf("unable to write tenant users data: %w", err)
		}
	}

	return
}
