package rds

import (
	"database/sql"
	"fmt"

	"time"

	"strings"

	"github.com/Financial-Times/factset-uploader/factset"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

const (
	MetadataTableCount = 2
)

type Client struct {
	DB     *sql.DB
	schema string
}

//
func NewClient(dsn string) (*Client, error) {
	schema := dsn[strings.LastIndex(dsn, "/")+1:]
	connString := fmt.Sprintf("%s?interpolateParams=true&parseTime=true&allowAllFiles=true", dsn)
	db, err := sql.Open("mysql", connString)
	if err != nil {
		log.WithError(err).Errorf("Error connecting to db: %s", connString)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.WithError(err).Errorf("Error establishing valid connection to db: %s", connString)
		return nil, err
	}

	return &Client{
		DB:     db,
		schema: schema,
	}, nil
}

//TODO in future we should have versioning/namespacing for our schema tables so that they are only dropped after a successful reload
func (c *Client) DropTablesWithProductAndBundle(product string, bundle string) error {
	getTableQuery := fmt.Sprintf(`SELECT tablename FROM metadata_table_version WHERE product = '%s' AND bundle = '%s'`, product, bundle)
	rows, err := c.DB.Query(getTableQuery)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error running query to return tables matching: product = %s & bundle = %s", product, bundle)
		return err
	}

	var tableNames []string

	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error scanning rows for tables matching: product = %s & bundle = %s", product, bundle)
			return err
		}
		tableNames = append(tableNames, tableName)
	}

	if len(tableNames) == 0 {
		log.WithFields(log.Fields{"fs_product": product}).Infof("Db has no tables matching: product = %s & bundle = %s", product, bundle)
		return nil
	}
	dropTableQuery := fmt.Sprintf(`DROP TABLES IF EXISTS %s`, strings.Join(tableNames, ", "))
	_, err = c.DB.Exec(dropTableQuery)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error executing query to drop tables matching: %s", strings.Join(tableNames, ", "))
		return err
	}
	return nil
}

func (c *Client) DropDataFromTable(tableName string, product string) error {
	deleteRowsQuery := fmt.Sprintf(`DELETE FROM %s`, tableName)
	_, err := c.DB.Exec(deleteRowsQuery)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error executing query to clear data from table: %s", tableName)
		return err
	}
	return nil
}

func (c *Client) UpdateLoadedTableVersion(tableName string, version factset.PackageVersion, pkg factset.Package) error {
	updateTableMetadataQueryTemplate := `REPLACE INTO metadata_table_version
						(tablename, feed_version, sequence, date_loaded, product, bundle)
						VALUES (?, ?, ?, NOW(), ?, ?)`
	stmt, err := c.DB.Prepare(updateTableMetadataQueryTemplate)
	defer stmt.Close()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("error preparing query to update table metadata for table: %s", tableName)
		return err
	}

	res, err := stmt.Exec(tableName, version.FeedVersion, version.Sequence, pkg.Product, pkg.Bundle)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("error running query to update table metadata for table: %s", tableName)
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if rowsAffected <= 0 {
		err := fmt.Errorf("no rows were updated as a result of running update table metadata for table: %s", tableName)
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Error(err)
		return err
	}
	return nil
}

func (c *Client) UpdateLoadedPackageVersion(packageMetadata *factset.PackageMetadata) error {
	var product = packageMetadata.Package.Product
	var bundle = packageMetadata.Package.Bundle
	updatePackageMetadataQueryTemplate := `REPLACE INTO metadata_package_version
						(product, bundle, schema_feed_version, schema_sequence, schema_date_loaded, package_feed_version, package_sequence, package_date_loaded)
						VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`
	stmt, err := c.DB.Prepare(updatePackageMetadataQueryTemplate)
	defer stmt.Close()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error preparing query to update package metadata for product: %s, bundle: %s", product, bundle)
		return err
	}

	res, err := stmt.Exec(product, bundle, packageMetadata.SchemaVersion.FeedVersion, packageMetadata.SchemaVersion.Sequence, packageMetadata.SchemaLoadedDate, packageMetadata.PackageVersion.FeedVersion, packageMetadata.PackageVersion.Sequence)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Error executing query to update package metadata for product: %s, bundle: %s", product, bundle)
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if rowsAffected <= 0 {
		err := fmt.Errorf("no rows were updated as a result of running update package metadata for product: %s, bundle: %s", product, bundle)
		log.WithFields(log.Fields{"fs_product": product}).Error(err)
	}
	return nil
}

func (c *Client) LoadTable(filename, table string) error {
	queryTemplate := `LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s FIELDS TERMINATED BY '|'
	OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;`

	_, err := c.DB.Exec(fmt.Sprintf(queryTemplate, filename, table))
	return err
}

func (c *Client) GetPackageMetadata(pkg factset.Package) (factset.PackageMetadata, error) {
	var pkgMetadata = factset.PackageMetadata{}
	queryTemplate := `SELECT product, bundle, schema_feed_version, schema_sequence, schema_date_loaded, package_feed_version, package_sequence, package_date_loaded
						FROM metadata_package_version
						WHERE product = ? AND bundle = ?`
	stmt, err := c.DB.Prepare(queryTemplate)
	defer stmt.Close()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error preparing query to return package metadata for product: %s", pkg.Product)
		return pkgMetadata, err
	}

	stmt.Exec()
	var product string
	var bundle string
	var schemaFeedVersion, schemaSequence, packageFeedVersion, packageSequence int
	var schemaDateLoaded, packageDateLoaded time.Time

	err = stmt.QueryRow(pkg.Product, pkg.Bundle).Scan(
		&product, &bundle, &schemaFeedVersion, &schemaSequence, &schemaDateLoaded,
		&packageFeedVersion, &packageSequence, &packageDateLoaded)

	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error executing scan of package metadata table for product: %s", pkg.Product)
		return pkgMetadata, err
	}
	return factset.PackageMetadata{
		Package: pkg,
		SchemaVersion: factset.PackageVersion{
			FeedVersion: schemaFeedVersion,
			Sequence:    schemaSequence,
		},
		SchemaLoadedDate: schemaDateLoaded,
		PackageVersion: factset.PackageVersion{
			FeedVersion: packageFeedVersion,
			Sequence:    packageSequence,
		},
		PackageLoadedDate: packageDateLoaded,
	}, nil
}

//
func (c *Client) LoadMetadataTables() error {
	query := `CREATE TABLE IF NOT EXISTS metadata_package_version (
			product varchar(255) NOT NULL,
			bundle varchar(255) NOT NULL,
			schema_feed_version INT,
			schema_sequence INT,
			schema_date_loaded DATETIME,
			package_feed_version INT,
			package_sequence INT,
			package_date_loaded DATETIME,
			PRIMARY KEY (product)
		);`
	if _, err := c.DB.Exec(query); err != nil {
		log.WithError(err).Error("Error running query to create metadata_package_version table")
		return err
	}

	query2 := `
		CREATE TABLE IF NOT EXISTS metadata_table_version (
			tablename varchar(255) NOT NULL,
			feed_version INT,
			sequence INT,
			date_loaded DATETIME,
			product varchar(255) NOT NULL,
			bundle  varchar(255) NOT NULL,
			PRIMARY KEY (tablename)
		);`

	if _, err := c.DB.Exec(query2); err != nil {
		log.WithError(err).Error("Error running query to create metadata_table_version table")
		return err
	}
	return nil
}

// CreateTablesFromSchema
// Takes the semicolon delimited contents of the create table file and creates the tables.
func (c *Client) CreateTablesFromSchema(contents []byte, pkg factset.Package) error {
	statements := strings.Split(string(contents), ";")

	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement != "" && len(statement) > 10 {
			statementSplits := strings.Split(statement, " ")
			_, err := c.DB.Exec(statement)
			if err != nil {
				if !(strings.Contains(err.Error(), fmt.Sprintf("Error 1050: Table '%s' already exists", statementSplits[2]))) {
					log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error running query to create schema for %s", pkg.Product)
					return err
				} else {
					log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Table %s has already been created by a different package", statementSplits[2])
					continue
				}
			}
			// update metadata table on creation of each schema table
			// if load is unsuccessful schema tables are cleaned up by subsequent loads
			if statementSplits[0] == "CREATE" && statementSplits[1] == "TABLE" {
				if err = c.UpdateLoadedTableVersion(statementSplits[2], factset.PackageVersion{0, 0}, pkg); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
