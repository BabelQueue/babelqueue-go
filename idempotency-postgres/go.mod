module github.com/babelqueue/babelqueue-go/idempotency-postgres

go 1.21

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/babelqueue/babelqueue-go v1.6.0
	github.com/jackc/pgx/v5 v5.7.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.17.0 // indirect
)

// In-repo development: resolve the core locally. Consumers ignore replace
// directives in dependencies and use the required version from the proxy.
replace github.com/babelqueue/babelqueue-go => ../
