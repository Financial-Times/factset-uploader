package rds

import (
	"database/sql"
	"fmt"

	"time"

	"strings"

	"github.com/Financial-Times/factset-uploader/factset"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	MetadataTableCount = 2
)

type Client struct {
	db     *sql.DB
	schema string
}

func (c *Client) DropTablesWithPrefix(prefix string) error {

	getTableQuery := fmt.Sprintf(`SHOW TABLES LIKE '%s%%'`, prefix)
	rows, err := c.db.Query(getTableQuery)
	if err != nil {
		return err
	}

	var tableNames []string

	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}
		tableNames = append(tableNames, tableName)
	}

	if len(tableNames) == 0 {
		return nil
	}
	dropTableQuery := fmt.Sprintf(`DROP TABLES %s`, strings.Join(tableNames, ", "))
	_, err = c.db.Exec(dropTableQuery)
	if err != nil {
		return err
	}

	return nil
}

func NewClient(host, user, pass, schema string) (*Client, error) {
	connString := fmt.Sprintf("%s:%s@%s/%s?interpolateParams=true&parseTime=true&allowAllFiles=true", user, pass, host, schema)
	db, err := sql.Open("mysql", connString)
	if err != nil {
		log.WithError(err).Errorf("Error connecting to db: %s", connString)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.Error(err)
		return nil, err
	}

	return &Client{
		db:     db,
		schema: schema,
	}, nil
}

func (c *Client) GetLoadedVersion(tableName string) (factset.PackageVersion, error) {
	queryTemplate := `SELECT feed_version, sequence
						FROM metadata_table_version
						WHERE tablename = ?
						`
	stmt, err := c.db.Prepare(queryTemplate)
	if err != nil {
		return factset.PackageVersion{}, err
	}
	var feedVersion int
	var sequence int
	err = stmt.QueryRow(tableName).Scan(&feedVersion, &sequence)
	defer stmt.Close()
	if err != nil {
		return factset.PackageVersion{}, err
	}
	return factset.PackageVersion{FeedVersion: feedVersion, Sequence: sequence}, nil
}

func (c *Client) UpdateLoadedTableVersion(tableName string, version factset.PackageVersion) error {
	queryTemplate := `REPLACE INTO metadata_table_version
						(tablename, feed_version, sequence, date_loaded)
						VALUES (?, ?, ?, NOW())`
	stmt, err := c.db.Prepare(queryTemplate)
	defer stmt.Close()
	if err != nil {
		return err
	}

	res, err := stmt.Exec(tableName, version.FeedVersion, version.Sequence)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if rowsAffected <= 0 {
		return errors.New("No row has been updated")
	}
	return nil
}

func (c *Client) VerifyMetadata() (bool, error) {
	queryTemplate := `SELECT count(*)
						FROM information_schema.TABLES
						WHERE TABLE_SCHEMA = ?
						AND TABLE_NAME LIKE "metadata%"`

	stmt, err := c.db.Prepare(queryTemplate)
	if err != nil {
		return false, err
	}
	var metadataTableCount int
	err = stmt.QueryRow(c.schema).Scan(&metadataTableCount)
	defer stmt.Close()
	if err != nil {
		return false, err
	}
	return metadataTableCount == MetadataTableCount, nil
}

func (c *Client) LoadTable(filename, table string) error {
	queryTemplate := `LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY '|'
	OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;`

	log.Info(filename, table)

	_, err := c.db.Exec(fmt.Sprintf(queryTemplate, filename, table))
	return err
}

func (c *Client) GetPackageMetadata(pkg factset.Package) (*factset.PackageMetadata, error) {
	queryTemplate := `SELECT package, feed_version, schema_sequence, schema_date_loaded, package_sequence, package_date_loaded
						FROM metadata_package_version
						WHERE package = ?`

	stmt, err := c.db.Prepare(queryTemplate)
	if err != nil {
		return nil, err
	}
	var packageName string
	var feedVersion, schemaSequence, packageSequence int
	var schemaDateLoaded, packageDateLoaded time.Time
	if err := stmt.QueryRow(pkg.Dataset).Scan(
		&packageName, &feedVersion, &schemaSequence,
		&schemaDateLoaded, &packageSequence, &packageDateLoaded); err != nil {
		return nil, err
	}
	return &factset.PackageMetadata{
		Package: pkg,
		SchemaVersion: factset.PackageVersion{
			FeedVersion: feedVersion,
			Sequence:    schemaSequence,
		},
		SchemaLoadedDate: schemaDateLoaded,
		PackageVersion: factset.PackageVersion{
			FeedVersion: feedVersion,
			Sequence:    packageSequence,
		},
		PackageLoadedDate: packageDateLoaded,
	}, nil
}

func (c *Client) LoadMetadataTables() error {

	query := `
		CREATE TABLE IF NOT EXISTS metadata_package_version (
			package varchar(255) NOT NULL,
			feed_version INT,
			schema_sequence INT,
			schema_date_loaded DATETIME,
			package_sequence INT,
			package_date_loaded DATETIME,
			PRIMARY KEY (package)
		);`
	_, err := c.db.Exec(query)
	if err != nil {
		return err
	}

	query2 := `
		CREATE TABLE IF NOT EXISTS metadata_table_version (
			tablename varchar(255) NOT NULL,
			feed_version INT,
			sequence INT,
			date_loaded DATETIME,
			PRIMARY KEY (tablename)
		);`

	_, err = c.db.Exec(query2)
	//_, err = c.db.Exec(fmt.Sprintf(query2, c.schema))
	return err
}
