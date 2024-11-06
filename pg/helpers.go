package pg

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

func tenantSchemaPrefix(dbName string, schemaName string) string {
	return fmt.Sprintf("%s_%s", dbName, schemaName)
}

func tenantSchemaGroupNames(dbName string, schemaName string) SchemaGroups {
	tenantSchemaPrefix := tenantSchemaPrefix(dbName, schemaName)

	admin := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, schemaAdminSuffix, groupSuffix)
	readwrite := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, rwSuffix, groupSuffix)
	readonly := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, roSuffix, groupSuffix)

	return SchemaGroups{
		Admin:     admin,
		ReadWrite: readwrite,
		ReadOnly:  readonly,
	}
}

func newTenantSchemaUserCredentials(dbName string, schemaName string) SchemaUsers {
	tenantSchemaPrefix := tenantSchemaPrefix(dbName, schemaName)

	adminUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, schemaAdminSuffix, userSuffix)
	adminPassword, _ := GenerateRandomPassword(PasswordConfig{})

	rwUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, rwSuffix, userSuffix)
	rwPassword, _ := GenerateRandomPassword(PasswordConfig{})

	roUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, roSuffix, userSuffix)
	roPassword, _ := GenerateRandomPassword(PasswordConfig{})

	admin := UserCredentials{
		Username: adminUsername,
		Password: adminPassword,
	}

	readwrite := UserCredentials{
		Username: rwUsername,
		Password: rwPassword,
	}

	readonly := UserCredentials{
		Username: roUsername,
		Password: roPassword,
	}

	return SchemaUsers{
		Admin:     admin,
		ReadWrite: readwrite,
		ReadOnly:  readonly,
	}
}

func GenerateRandomPassword(config PasswordConfig) (string, error) {
	const (
		letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		numbers       = "0123456789"
		specialChars  = "!#$%^&*()-_=+[]{}|;:,.<>?~`"
		defaultLength = 32
	)

	var charset string

	if config.UseLetters {
		charset += letters
	}

	if config.UseNum {
		charset += numbers
	}

	if config.UseSpecial {
		charset += specialChars
	}

	if config.ExcludeSpecial != "" {
		for _, char := range config.ExcludeSpecial {
			charset = strings.ReplaceAll(charset, string(char), "")
		}
	}

	if charset == "" {
		charset += letters
		charset += numbers
	}

	if config.Length == 0 {
		config.Length = defaultLength
	}

	password := make([]byte, config.Length)
	for i := range password {
		randomIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", fmt.Errorf("unable to generate random index: %w", err)
		}
		password[i] = charset[randomIndex.Int64()]
	}

	return string(password), nil
}
