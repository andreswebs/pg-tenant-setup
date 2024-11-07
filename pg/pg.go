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

func (pg *Postgres) CheckIfRoleExists(ctx context.Context, roleName string) bool {
	exists := false
	var res int
	err := pg.db.QueryRow(ctx, fmt.Sprintf("SELECT 1 FROM pg_roles WHERE rolname='%s';", roleName)).Scan(&res)
	if err != nil && (err.Error() != "no rows in result set" || err != pgx.ErrNoRows) {
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
	if err != nil && (err.Error() != "no rows in result set" || err != pgx.ErrNoRows) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	if res == 1 {
		exists = true
	}
	return exists
}

func (pg *Postgres) NewTenantSchemaGroups(ctx context.Context, roleNamePrefix string, schemaName string) SchemaGroups {

	ownerRole := tenantOwnerName(roleNamePrefix)

	schemaGroups := tenantSchemaGroupNames(roleNamePrefix, schemaName)
	schemaUsers := newTenantSchemaUserCredentials(roleNamePrefix, schemaName)

	adminGroupExists := pg.CheckIfRoleExists(ctx, schemaGroups.Admin)
	rwGroupExists := pg.CheckIfRoleExists(ctx, schemaGroups.ReadWrite)
	roGroupExists := pg.CheckIfRoleExists(ctx, schemaGroups.ReadOnly)

	adminUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.Admin.Username)
	rwUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.ReadWrite.Username)
	roUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.ReadOnly.Username)

	dropOwnedByAdminUser := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.Admin.Username, ownerRole, schemaUsers.Admin.Username, schemaUsers.Admin.Username)
	dropAdminUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.Admin.Username)

	dropOwnedByAdminGroup := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.Admin, ownerRole, schemaGroups.Admin, schemaGroups.Admin)
	dropAdminGroup := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.Admin)
	createAdminGroup := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.Admin)

	dropOwnedByRWUser := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadWrite.Username, ownerRole, schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Username)
	dropRWUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadWrite.Username)

	dropOwnedByRWGroup := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.ReadWrite, ownerRole, schemaGroups.ReadWrite, schemaGroups.ReadWrite)
	dropRWGroup := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadWrite)
	createRWGroup := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadWrite)

	dropOwnedByROUser := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadOnly.Username, ownerRole, schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Username)
	dropROUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadOnly.Username)

	dropOwnedByROGroup := fmt.Sprintf("REASSIGN OWNED BY %s TO %s; SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaGroups.ReadOnly, ownerRole, schemaGroups.ReadOnly, schemaGroups.ReadOnly)
	dropROGroup := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaGroups.ReadOnly)
	createROGroup := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", schemaGroups.ReadOnly)

	if adminUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByAdminUser)
	}
	pg.RunExec(pg.db, ctx, dropAdminUser)

	if adminGroupExists {
		pg.RunExec(pg.db, ctx, dropOwnedByAdminGroup)
	}
	pg.RunExec(pg.db, ctx, dropAdminGroup)
	pg.RunExec(pg.db, ctx, createAdminGroup)

	if rwUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByRWUser)
	}
	pg.RunExec(pg.db, ctx, dropRWUser)

	if rwGroupExists {
		pg.RunExec(pg.db, ctx, dropOwnedByRWGroup)
	}
	pg.RunExec(pg.db, ctx, dropRWGroup)
	pg.RunExec(pg.db, ctx, createRWGroup)

	if roUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByROUser)
	}
	pg.RunExec(pg.db, ctx, dropROUser)

	if roGroupExists {
		pg.RunExec(pg.db, ctx, dropOwnedByROGroup)
	}
	pg.RunExec(pg.db, ctx, dropROGroup)
	pg.RunExec(pg.db, ctx, createROGroup)

	return schemaGroups
}

func (pg *Postgres) NewTenantSchemaUsers(ctx context.Context, roleNamePrefix string, schemaName string) SchemaUsers {

	schemaGroups := tenantSchemaGroupNames(roleNamePrefix, schemaName)
	schemaUsers := newTenantSchemaUserCredentials(roleNamePrefix, schemaName)

	adminUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.Admin.Username)
	rwUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.ReadWrite.Username)
	roUserExists := pg.CheckIfRoleExists(ctx, schemaUsers.ReadOnly.Username)

	dropOwnedByAdminUser := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.Admin.Username, schemaUsers.Admin.Username)
	dropAdminUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.Admin.Username)
	createAdminUser := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.Admin.Username, schemaUsers.Admin.Password)
	grantAdmin := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.Admin, schemaUsers.Admin.Username)

	dropOwnedByRWUser := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Username)
	dropRWUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadWrite.Username)
	createRWUser := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadWrite.Username, schemaUsers.ReadWrite.Password)
	grantRW := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadWrite, schemaUsers.ReadWrite.Username)

	dropOwnedByROUser := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Username)
	dropROUser := fmt.Sprintf("DROP ROLE IF EXISTS %s;", schemaUsers.ReadOnly.Username)
	createROUser := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD '%s';", schemaUsers.ReadOnly.Username, schemaUsers.ReadOnly.Password)
	grantRO := fmt.Sprintf("GRANT %s TO %s;", schemaGroups.ReadOnly, schemaUsers.ReadOnly.Username)

	if adminUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByAdminUser)
	}
	pg.RunExec(pg.db, ctx, dropAdminUser)
	pg.RunExec(pg.db, ctx, createAdminUser)
	pg.RunExec(pg.db, ctx, grantAdmin)

	if rwUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByRWUser)
	}
	pg.RunExec(pg.db, ctx, dropRWUser)
	pg.RunExec(pg.db, ctx, createRWUser)
	pg.RunExec(pg.db, ctx, grantRW)

	if roUserExists {
		pg.RunExec(pg.db, ctx, dropOwnedByROUser)
	}
	pg.RunExec(pg.db, ctx, dropROUser)
	pg.RunExec(pg.db, ctx, createROUser)
	pg.RunExec(pg.db, ctx, grantRO)

	return schemaUsers
}

func (pg *Postgres) NewTenantDB(ctx context.Context, dbName string, tenantName string) (err error) {

	roleNamePrefix := tenantName
	if roleNamePrefix == "" {
		roleNamePrefix = dbName
	}

	ownerRole := tenantOwnerName(roleNamePrefix)

	var currentRole string
	err = pg.db.QueryRow(ctx, "SELECT current_role").Scan(&currentRole)
	if err != nil {
		return
	}

	dbExists := pg.CheckIfDBExists(ctx, dbName)
	ownerRoleExists := pg.CheckIfRoleExists(ctx, ownerRole)

	// begin definitions

	alterDBToCurrent := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, currentRole)
	dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName)
	dropOwned := fmt.Sprintf("SET ROLE %s; DROP OWNED BY %s; RESET ROLE;", ownerRole, ownerRole)
	dropOwner := fmt.Sprintf("DROP ROLE IF EXISTS %s;", ownerRole)
	createOwner := fmt.Sprintf("CREATE ROLE %s WITH NOLOGIN;", ownerRole)
	createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
	alterDB := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s;", dbName, ownerRole)

	// revoke all privileges from PUBLIC

	revokeDBPublic := fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC;", dbName)
	revokeSchemaPublic := fmt.Sprintf("REVOKE CREATE ON SCHEMA public FROM PUBLIC;")

	// begin executions

	if dbExists {
		pg.RunExec(pg.db, ctx, alterDBToCurrent)
	}

	pg.RunExec(pg.db, ctx, dropDB)

	if ownerRoleExists {
		pg.RunExec(pg.db, ctx, dropOwned)
	}

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

	// basic schema definitions

	dropSchema := fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schemaName)
	createSchema := fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
	revokeCreate := fmt.Sprintf("REVOKE CREATE ON SCHEMA %s FROM PUBLIC;", schemaName)

	pg.RunExec(conn, ctx, dropSchema)

	_, err = pg.RunExec(conn, ctx, createSchema)
	if err != nil {
		err = fmt.Errorf("unable to create schema: %w", err)
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

	tenantUsers := pg.NewTenantSchemaUsers(ctx, roleNamePrefix, schemaName)

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
