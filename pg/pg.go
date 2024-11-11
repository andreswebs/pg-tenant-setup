package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	db       *pgxpool.Pool
	roleName string
}

var (
	pgInstance    *Postgres
	pgOnce        sync.Once
	pgCurrentRole string
)

func Connect(ctx context.Context, connString string) (*Postgres, error) {
	pgOnce.Do(func() {
		db, err := pgxpool.New(ctx, connString)
		if err != nil {
			err = fmt.Errorf("unable to create connection pool: %w", err)
			return
		}

		var currentRole string
		err = db.QueryRow(ctx, "SELECT current_role").Scan(&currentRole)
		if err != nil {
			return
		}

		pgInstance = &Postgres{db, currentRole}
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

	outSQLFile := os.Getenv(envVarOutSQLFile)

	config.BeforeConnect = func(ctx context.Context, connConfig *pgx.ConnConfig) (err error) {
		if outSQLFile != "" {
			appendToFile(outSQLFile, fmt.Sprintf("-- connecting to database %s\n", connConfig.Database))
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
			if outSQLFile != "" {
				appendToFile(outSQLFile, fmt.Sprintf("-- closing connection to database %s\n", conn.Config().Database))
			}
		}
	} else {
		config.BeforeClose = func(conn *pgx.Conn) {
			if outSQLFile != "" {
				appendToFile(outSQLFile, fmt.Sprintf("-- closing connection to database %s\n", conn.Config().Database))
			}
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

func (pg *Postgres) CheckIfRoleExists(ctx context.Context, roleName string) bool {
	exists := false
	var res int
	err := pg.db.QueryRow(ctx, fmt.Sprintf("SELECT 1 FROM pg_roles WHERE rolname='%s';", roleName)).Scan(&res)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	if res == 1 {
		exists = true
	}
	return exists
}

func (pg *Postgres) CheckIfDBExists(ctx context.Context, dbName string) bool {
	exists := false
	var res int
	err := pg.db.QueryRow(ctx, fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname='%s';", dbName)).Scan(&res)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	if res == 1 {
		exists = true
	}
	return exists
}

func (pg *Postgres) DropRole(ctx context.Context, roleName string) {
	dropOwnedByRole := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", roleName, pg.roleName, roleName, roleName)
	dropRole := fmt.Sprintf("DROP ROLE IF EXISTS %s;", roleName)

	roleExists := pg.CheckIfRoleExists(ctx, roleName)
	if roleExists {
		pg.RunExec(pg.db, ctx, dropOwnedByRole)
		pg.RunExec(pg.db, ctx, dropRole)
	}
}

func (pg *Postgres) DropTenantSchemaUsers(ctx context.Context, roleNamePrefix string, schemaName string) {
	schemaUsers := newTenantSchemaUserCredentials(roleNamePrefix, schemaName)

	pg.DropRole(ctx, schemaUsers.ReadOnly.Username)
	pg.DropRole(ctx, schemaUsers.ReadWrite.Username)
	pg.DropRole(ctx, schemaUsers.Admin.Username)
}

func (pg *Postgres) DropTenantSchemaGroups(ctx context.Context, roleNamePrefix string, schemaName string) {
	pg.DropTenantSchemaUsers(ctx, roleNamePrefix, schemaName)

	schemaGroups := tenantSchemaGroupNames(roleNamePrefix, schemaName)

	pg.DropRole(ctx, schemaGroups.ReadOnly)
	pg.DropRole(ctx, schemaGroups.ReadWrite)
	pg.DropRole(ctx, schemaGroups.Admin)
}

func (pg *Postgres) DropDB(ctx context.Context, dbName string) {
	alterDB := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, pg.roleName)
	dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName)

	dbExists := pg.CheckIfDBExists(ctx, dbName)
	if dbExists {
		pg.RunExec(pg.db, ctx, alterDB)
		pg.RunExec(pg.db, ctx, dropDB)
	}
}

func (pg *Postgres) CreateGroup(ctx context.Context, groupname string) (err error) {
	createGroup := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", groupname)
	_, err = pg.RunExec(pg.db, ctx, createGroup)
	return
}

func (pg *Postgres) NewTenantSchemaGroups(ctx context.Context, roleNamePrefix string, schemaName string) SchemaGroups {
	pg.DropTenantSchemaGroups(ctx, roleNamePrefix, schemaName)

	schemaGroups := tenantSchemaGroupNames(roleNamePrefix, schemaName)

	pg.CreateGroup(ctx, schemaGroups.Admin)
	pg.CreateGroup(ctx, schemaGroups.ReadWrite)
	pg.CreateGroup(ctx, schemaGroups.ReadOnly)

	return schemaGroups
}

func (pg *Postgres) CreateUser(ctx context.Context, user UserCredentials, groupname string) (err error) {
	createUser := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", user.Username, user.Password)
	grantGroup := fmt.Sprintf("GRANT %s TO %s;", groupname, user.Username)

	_, err = pg.RunExec(pg.db, ctx, createUser)

	if groupname != "" {
		_, err = pg.RunExec(pg.db, ctx, grantGroup)
	}

	return
}

func (pg *Postgres) NewTenantSchemaUsers(ctx context.Context, roleNamePrefix string, schemaName string) SchemaUsers {
	pg.DropTenantSchemaUsers(ctx, roleNamePrefix, schemaName)

	schemaGroups := tenantSchemaGroupNames(roleNamePrefix, schemaName)
	schemaUsers := newTenantSchemaUserCredentials(roleNamePrefix, schemaName)

	pg.CreateUser(ctx, schemaUsers.Admin, schemaGroups.Admin)
	pg.CreateUser(ctx, schemaUsers.ReadWrite, schemaGroups.ReadWrite)
	pg.CreateUser(ctx, schemaUsers.ReadOnly, schemaGroups.ReadOnly)

	return schemaUsers
}

func (pg *Postgres) NewTenantDB(ctx context.Context, dbName string, tenantName string) (err error) {

	roleNamePrefix := tenantName
	if roleNamePrefix == "" {
		roleNamePrefix = dbName
	}

	ownerRole := tenantOwnerName(roleNamePrefix)

	// begin definitions

	createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
	alterDB := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, ownerRole)

	// revoke all privileges from PUBLIC
	revokeDBPublic := fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC;", dbName)
	revokeSchemaPublic := fmt.Sprintf("REVOKE CREATE ON SCHEMA public FROM PUBLIC;")

	// begin executions

	pg.DropDB(ctx, dbName)
	pg.DropRole(ctx, ownerRole)

	err = pg.CreateGroup(ctx, ownerRole)
	if err != nil {
		err = fmt.Errorf("unable to create owner role: %w", err)
		return
	}

	_, err = pg.RunExec(pg.db, ctx, createDB)
	if err != nil {
		err = fmt.Errorf("unable to create database: %w", err)
		pg.DropRole(ctx, ownerRole)
		return
	}

	_, err = pg.RunExec(pg.db, ctx, alterDB)
	if err != nil {
		err = fmt.Errorf("unable to set database owner: %w", err)
		pg.DropDB(ctx, dbName)
		pg.DropRole(ctx, ownerRole)
		return
	}

	// execute revoke all privileges from PUBLIC
	pg.RunExec(pg.db, ctx, revokeDBPublic)
	err = func() (err error) {
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
	}()

	return
}

func (pg *Postgres) NewTenantSchema(ctx context.Context, schemaName string, tenantName string, connConfig ConnectDBConfig) (err error) {

	if connConfig.DBName == "" {
		err = fmt.Errorf("missing database name: %w", err)
		return
	}

	dbName := connConfig.DBName

	roleNamePrefix := tenantName
	if roleNamePrefix == "" {
		roleNamePrefix = dbName
	}

	ownerRole := tenantOwnerName(roleNamePrefix)

	if connConfig.RoleName == "" {
		connConfig.RoleName = ownerRole
	}

	dropSchema := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schemaName)
	createSchema := fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
	revokeCreateOnSchema := fmt.Sprintf("REVOKE CREATE ON SCHEMA %s FROM PUBLIC;", schemaName)

	err = func() (err error) {
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

		pg.RunExec(conn, ctx, dropSchema)

		_, err = pg.RunExec(conn, ctx, createSchema)
		if err != nil {
			err = fmt.Errorf("unable to create schema: %w", err)
			return
		}

		pg.RunExec(conn, ctx, revokeCreateOnSchema)

		return
	}()

	if err != nil {
		return
	}

	tenantGroups := pg.NewTenantSchemaGroups(ctx, roleNamePrefix, schemaName)

	// grant basic privileges
	grantDBAccess := fmt.Sprintf(
		"GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s;",
		dbName, fmt.Sprintf("%s, %s, %s", tenantGroups.Admin, tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)
	pg.RunExec(pg.db, ctx, grantDBAccess)

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

	// begin executions

	err = func() (err error) {
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

		return
	}()

	if err != nil {
		return
	}

	tenantUsers := pg.NewTenantSchemaUsers(ctx, roleNamePrefix, schemaName)

	func() {
		outCredsFile := os.Getenv(envVarOutCredsFile)

		if outCredsFile != "" {
			tenantUsersData, err := json.Marshal(tenantUsers)
			if err != nil {
				err = fmt.Errorf("unable to marshal tenant users data: %w", err)
				fmt.Fprintf(os.Stderr, "%v\n", err)
			}

			err = os.WriteFile(outCredsFile, tenantUsersData, outFileMode)
			if err != nil {
				err = fmt.Errorf("unable to write tenant users data: %w", err)
				fmt.Fprintf(os.Stderr, "%v\n", err)
			}
		}
	}()

	return
}
