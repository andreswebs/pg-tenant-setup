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

	Admin := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, schemaAdminSuffix, groupSuffix)
	ReadWrite := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, rwSuffix, groupSuffix)
	ReadOnly := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, roSuffix, groupSuffix)

	return SchemaGroups{Admin, ReadOnly, ReadWrite}
}

func newTenantSchemaUserCredentials(dbName string, schemaName string) SchemaUsers {
	tenantSchemaPrefix := tenantSchemaPrefix(dbName, schemaName)

	adminUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, schemaAdminSuffix, userSuffix)
	adminPassword, _ := GenerateRandomPassword(PasswordConfig{})

	rwUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, rwSuffix, userSuffix)
	rwPassword, _ := GenerateRandomPassword(PasswordConfig{})

	roUsername := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, roSuffix, userSuffix)
	roPassword, _ := GenerateRandomPassword(PasswordConfig{})

	Admin := UserCredentials{
		Username: adminUsername,
		Password: adminPassword,
	}

	ReadWrite := UserCredentials{
		Username: rwUsername,
		Password: rwPassword,
	}

	ReadOnly := UserCredentials{
		Username: roUsername,
		Password: roPassword,
	}

	return SchemaUsers{Admin, ReadWrite, ReadOnly}
}

func GenerateRandomPassword(config PasswordConfig) (string, error) {
	const (
		letters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		numbers      = "0123456789"
		specialChars = "!#$%^&*()-_=+[]{}|;:,.<>?~`"
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
		config.Length = 32
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
