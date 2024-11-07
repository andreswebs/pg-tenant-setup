package pg

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"
)

func tenantOwnerName(roleNamePrefix string) string {
	return fmt.Sprintf("%s%s", roleNamePrefix, ownerSuffix)
}

func tenantSchemaPrefix(roleNamePrefix string, schemaName string) string {
	return fmt.Sprintf("%s_%s", roleNamePrefix, schemaName)
}

func tenantSchemaGroupNames(roleNamePrefix string, schemaName string) SchemaGroups {
	tenantSchemaPrefix := tenantSchemaPrefix(roleNamePrefix, schemaName)

	admin := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, schemaAdminSuffix, groupSuffix)
	readwrite := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, rwSuffix, groupSuffix)
	readonly := fmt.Sprintf("%s%s%s", tenantSchemaPrefix, roSuffix, groupSuffix)

	return SchemaGroups{
		Admin:     admin,
		ReadWrite: readwrite,
		ReadOnly:  readonly,
	}
}

func newTenantSchemaUserCredentials(roleNamePrefix string, schemaName string) SchemaUsers {
	tenantSchemaPrefix := tenantSchemaPrefix(roleNamePrefix, schemaName)

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

func appendToFile(filename string, content string) {
	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, outFileMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	defer f.Close()

	if _, err := f.WriteString(content); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func truncateFile(filename string) {
	if err := os.Truncate(filename, 0); err != nil {
		fmt.Fprintf(os.Stderr, "failed: %v\n", err)
	}
}
