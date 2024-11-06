package main

import (
	"context"
	"fmt"
	"os"

	"github.com/andreswebs/pg-tenant-setup/pg"
	"github.com/jxskiss/mcli"
)

type CommonArgs struct {
	ConnectionString string `cli:"-c, --connection-string, PostgreSQL connection string" env:"PG_TENANT_SETUP_CONNECTION_STRING"`
	OutputSQLFile    string `cli:"#E, File name to save executed SQL commands to" env:"PG_TENANT_SETUP_OUTPUT_SQL_FILE"`
	HaltOnError      string `cli:"#E, Whether to halt SQL further execution on error" env:"PG_TENANT_SETUP_HALT_ON_ERROR"`
	DBName           string `cli:"#R, -d, --database-name, Database name"`
}

func main() {
	mcli.Add("create-database", createDB, "Create a new tenant database with an owner role.")
	mcli.Add("create-schema", createSchema, "Create a new tenant schema with a set of scoped roles.")
	mcli.AddCompletion()
	mcli.Run()
}

func createDB() {
	var args struct {
		CommonArgs
	}
	mcli.Parse(&args)

	ctx := context.Background()

	pgInstance, err := pg.Connect(ctx, args.ConnectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pgInstance.Close()

	err = pgInstance.Ping(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	err = pgInstance.NewTenantDB(ctx, args.DBName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create new tenant objects: %v\n", err)
		os.Exit(1)
	}
}

func createSchema() {
	var args struct {
		SchemaName            string `cli:"#R, -s, --schema-name, Schema name"`
		OutputCredentialsFile string `cli:"#E, File name to save schema users credentials to" env:"PG_TENANT_SETUP_OUTPUT_CREDENTIALS_FILE"`
		CommonArgs
	}
	mcli.Parse(&args)

	ctx := context.Background()

	pgInstance, err := pg.Connect(ctx, args.ConnectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pgInstance.Close()

	err = pgInstance.Ping(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	err = pgInstance.NewTenantSchema(ctx, args.SchemaName, pg.ConnectDBConfig{DBName: args.DBName})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create new tenant objects: %v\n", err)
		os.Exit(1)
	}
}
