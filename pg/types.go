package pg

const (
	ownerSuffix        = "_owner"
	schemaAdminSuffix  = "_schadm"
	roSuffix           = "_ro"
	rwSuffix           = "_rw"
	groupSuffix        = "_grp"
	userSuffix         = "_usr"
	envVarOutCredsFile = "PG_TENANT_SETUP_OUTPUT_CREDENTIALS_FILE"
	outFileMode        = 0600
)

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
