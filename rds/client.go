package rds

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type Client struct {
	db *sql.DB
}

func NewClient(host, user, pass, table string) *Client {
	connString := fmt.Sprintf("%s:%s@%s/%s", user, pass, host, table)
	db, err := sql.Open("mysql", connString)
	if err != nil {
		log.WithError(err).Errorf("Error connecting to db: %s", connString)
	}

	return &Client{
		db: db,
	}
}

func (c *Client) LoadTable(filename, table string) {
	queryTemplate := `LOAD DATA INFILE '%s'
					REPLACE
					INTO TABLE %s
					FIELDS
						TERMINATED BY '|'
						OPTIONALLY ENCLOSED BY '"'
					LINES
						TERMINATED BY '\n'
					IGNORE 1 LINES;`

}
