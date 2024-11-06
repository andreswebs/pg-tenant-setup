package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5"
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

	return pgInstance, nil
}

func (pg *Postgres) ConnectDB(ctx context.Context, connConfig ConnectDBConfig) (pool *pgxpool.Pool, err error) {
	config := pg.db.Config().Copy()
	if connConfig.DBName != "" {
		config.ConnConfig.Database = connConfig.DBName
	}

	if connConfig.RoleName != "" {
		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			setRole := fmt.Sprintf("SET ROLE %s;", connConfig.RoleName)
			conn.Exec(ctx, setRole)
			return nil
		}

		config.BeforeClose = func(conn *pgx.Conn) {
			resetRole := fmt.Sprintf("RESET ROLE;")
			conn.Exec(ctx, resetRole)
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

func (pg *Postgres) Ping(ctx context.Context) error {
	return pg.db.Ping(ctx)
}

func (pg *Postgres) Close() {
	pg.db.Close()
}

func (pg *Postgres) NewTenantSchemaGroups(ctx context.Context, dbName string, schemaName string) SchemaGroups {

	schemaGroups := tenantSchemaGroupNames(dbName, schemaName)

	dropOwnedByAdmin := fmt.Sprintf("DROP OWNED BY %s;", schemaGroups.Admin)
	dropAdmin := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.Admin)
	createAdmin := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.Admin)

	dropOwnedByRW := fmt.Sprintf("DROP OWNED BY %s;", schemaGroups.ReadWrite)
	dropRW := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadWrite)
	createRW := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadWrite)

	dropOwnedByRO := fmt.Sprintf("DROP OWNED BY %s;", schemaGroups.ReadOnly)
	dropRO := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadOnly)
	createRO := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadOnly)

	pg.db.Exec(ctx, dropOwnedByAdmin)
	pg.db.Exec(ctx, dropAdmin)
	pg.db.Exec(ctx, createAdmin)

	pg.db.Exec(ctx, dropOwnedByRW)
	pg.db.Exec(ctx, dropRW)
	pg.db.Exec(ctx, createRW)

	pg.db.Exec(ctx, dropOwnedByRO)
	pg.db.Exec(ctx, dropRO)
	pg.db.Exec(ctx, createRO)

	return schemaGroups
}

func (pg *Postgres) NewTenantSchemaUsers(ctx context.Context, dbName string, schemaName string) SchemaUsers {

	schemaGroups := tenantSchemaGroupNames(dbName, schemaName)
	schemaUsers := newTenantSchemaUserCredentials(dbName, schemaName)

	dropOwnedByAdmin := fmt.Sprintf("DROP OWNED BY %s;", schemaUsers.Admin.Username)
	dropAdmin := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.Admin.Username)
	createAdmin := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.Admin.Username, schemaUsers.Admin.Password)
	grantAdmin := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.Admin, schemaUsers.Admin.Username)

	dropOwnedByRW := fmt.Sprintf("DROP OWNED BY %s;", schemaUsers.ReadWrite.Username)
	dropRW := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadWrite.Username)
	createRW := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Password)
	grantRW := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadWrite, schemaUsers.ReadWrite.Username)

	dropOwnedByRO := fmt.Sprintf("DROP OWNED BY %s;", schemaUsers.ReadOnly.Username)
	dropRO := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadOnly.Username)
	createRO := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Password)
	grantRO := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadOnly, schemaUsers.ReadOnly.Username)

	pg.db.Exec(ctx, dropOwnedByAdmin)
	pg.db.Exec(ctx, dropAdmin)
	pg.db.Exec(ctx, createAdmin)
	pg.db.Exec(ctx, grantAdmin)

	pg.db.Exec(ctx, dropOwnedByRW)
	pg.db.Exec(ctx, dropRW)
	pg.db.Exec(ctx, createRW)
	pg.db.Exec(ctx, grantRW)

	pg.db.Exec(ctx, dropOwnedByRO)
	pg.db.Exec(ctx, dropRO)
	pg.db.Exec(ctx, createRO)
	pg.db.Exec(ctx, grantRO)

	return schemaUsers
}

func (pg *Postgres) NewTenantDB(ctx context.Context, dbName string) (err error) {
	ownerRole := fmt.Sprintf("%s%s", dbName, ownerSuffix)

	// begin definitions

	dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName)
	dropOwned := fmt.Sprintf("DROP OWNED BY %s;", ownerRole)
	dropOwner := fmt.Sprintf("DROP ROLE IF EXISTS %s;", ownerRole)
	createOwner := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", ownerRole)
	createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
	alterDB := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, ownerRole)

	// revoke all privileges from PUBLIC

	revokeDBPublic := fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC;", dbName)
	revokeSchemaPublic := fmt.Sprintf("REVOKE CREATE ON SCHEMA public FROM PUBLIC;")

	// begin executions

	pg.db.Exec(ctx, dropDB)
	pg.db.Exec(ctx, dropOwned)
	pg.db.Exec(ctx, dropOwner)

	_, err = pg.db.Exec(ctx, createOwner)
	if err != nil {
		err = fmt.Errorf("unable to create owner role: %w", err)
		return
	}

	_, err = pg.db.Exec(ctx, createDB)
	if err != nil {
		err = fmt.Errorf("unable to create database: %w", err)
		pg.db.Exec(ctx, dropOwner)
		return
	}

	_, err = pg.db.Exec(ctx, alterDB)
	if err != nil {
		err = fmt.Errorf("unable to set database owner: %w", err)
		pg.db.Exec(ctx, dropDB)
		pg.db.Exec(ctx, dropOwner)
		return
	}

	// execute revoke all privileges from PUBLIC

	pg.db.Exec(ctx, revokeDBPublic)

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

	conn.Exec(ctx, revokeSchemaPublic)

	return
}

func (pg *Postgres) NewTenantSchema(ctx context.Context, schemaName string, connConfig ConnectDBConfig) (err error) {

	if connConfig.DBName == "" {
		err = fmt.Errorf("missing database name: %w", err)
		return
	}

	dbName := connConfig.DBName
	ownerRole := fmt.Sprintf("%s%s", dbName, ownerSuffix)

	if connConfig.RoleName == "" {
		connConfig.RoleName = ownerRole
	}

	tenantGroups := pg.NewTenantSchemaGroups(ctx, dbName, schemaName)

	// grant basic privileges
	grantDBAccess := fmt.Sprintf(
		"GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s;",
		dbName, fmt.Sprintf("%s, %s, %s", tenantGroups.Admin, tenantGroups.ReadWrite, tenantGroups.ReadOnly),
	)
	pg.db.Exec(ctx, grantDBAccess)

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

	conn.Exec(ctx, dropSchema)

	_, err = conn.Exec(ctx, createSchema)
	if err != nil {
		err = fmt.Errorf("unable to create schema: %w", err)
		return
	}

	// begin executions

	conn.Exec(ctx, revokeCreate)
	conn.Exec(ctx, grantSchemaAdminCreate)
	conn.Exec(ctx, grantSchemaAdminTables)
	conn.Exec(ctx, grantSchemaAdminSequences)

	conn.Exec(ctx, grantSchemaUsage)
	conn.Exec(ctx, grantTablesRead)
	conn.Exec(ctx, grantSequencesRead)

	conn.Exec(ctx, grantDefaultSequencesRead)
	conn.Exec(ctx, grantDefaultSequencesWrite)
	conn.Exec(ctx, grantDefaultTablesRead)
	conn.Exec(ctx, grantDefaultTablesReadWrite)

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
