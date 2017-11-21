# Factset Uploader (factset-uploader)

[![Circle CI](https://circleci.com/gh/Financial-Times/factset-uploader/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/factset-uploader/tree/master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/factset-uploader)](https://goreportcard.com/report/github.com/Financial-Times/factset-uploader)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/factset-uploader/badge.svg)](https://coveralls.io/github/Financial-Times/factset-uploader)

## Introduction

The service is run on a timer; on start up it cycles through each of the following steps for each configured package.
Firstly it checks Factset SFTP server for most recent version of the package and associated schema.
It then compares the current and loaded schema and reloads the schema and all package data if found to be out-of-date.
If the schema is up-to-date then then only the data tables are completely reloaded.
If an error occurs during a package load the error is logged and service moves on to the next package.
Once complete the service shuts down.

In the future this service will handle delta files by updating data tables as opposed to doing full reloads.

## Package

### Data

Currently there are several categories of files from factset that are of interest to us:

1. Entity information that is used for our organisation information
2. People information that is used for our people and membership information
3. Reference information to provide on things such as controlled vocabulary i.e Role names
4. Symbology information to provide other identifiers such as the financial instruments
5. Fundamental information to provide information on the financials for entities

### Structure

All configured packages should be separated by a `;` with each distinct package composed of five distinct parts separated by `,` :

```
Dataset,FSPackage,Product,Bundle,Version;...
```
        
for example: for the package ` /datafeeds/fundamentals/ff_advanced_ap_v3/ff_advanced_der_ap_v3_full_1234.zip`
        
```
ff,fundamentals,ff_advanced_ap_v3,ff_advanced_der_ap,3;...
```

## Installation

Download the source code, dependencies and test dependencies:

```shell
go get -u github.com/kardianos/govendor
go get -u github.com/Financial-Times/factset-uploader
cd $GOPATH/src/github.com/Financial-Times/factset-uploader
govendor sync
go build .
```

## Running locally

This needs to be deployed to be able to connect to the the Factset SFTP which has white listed ip addresses

1. Install MySql
    brew info mysql56
    OSX:
    ```
    $ brew install mysql@5.6
    $ echo 'export PATH="/usr/local/opt/mysql@5.6/bin:$PATH"' >> ~/.bash_profile

    $ ps -ef | grep mysql
    /usr/local/opt/mysql@5.6/bin/mysqld --basedir=/usr/local/opt/mysql@5.6 --datadir=/usr/local/var/mysql --plugin-dir=/usr/local/opt/mysql@5.6/lib/plugin --log-error=ft-mw4758.ad.ft.com.err --pid-file=ft-mw4758.ad.ft.com.pid

    # When you open SQL Workbench there should be a default localhost database that you can click on.
    # We had some issues initially with this and we found that if you did the following it fixed it:

    $ launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.mysql.plist
    $ brew remove mysql
    $ rm -rf /usr/local/var/mysql/
    $ brew install mysql@5.6
    ```

    Windows:

        https://dev.mysql.com/downloads/mysql/5.6.html#downloads


2. Install MySql Workbench (Optional)

    https://dev.mysql.com/downloads/workbench/

3. Run the tests and install the binary:

        govendor sync
        govendor test -v -race
        go install

4. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/factset-uploader [--help]

Options:

        --app-system-code="factset-uploader"        System Code of the application ($APP_SYSTEM_CODE)
        --app-name="factset-uploader"               Application name ($APP_NAME)
        --log-level=info
        --factsetUser=xxx
        --factsetKey=xxx
        --factsetFTP=fts-sftp.factset.com
        --factsetPort=6671
        --packages=Dataset,FSPackage,Product,Bundle,Version;...
        --rds_dsn=<db_username>:<db_password>@tcp(<rds_url)/<database_name>     Details of the Aurora DB

The resources argument specifies a comma separated list of archives and files within that archive to be downloaded from Factset FTP server.
        
3. Test:

## Build and deployment
* Built by Docker Hub on merge to master: [coco/factset-uploader](https://hub.docker.com/r/coco/factset-uploader/)
* CI provided by CircleCI: [factset-uploader](https://circleci.com/gh/Financial-Times/factset-uploader)

## Service endpoints
There are no service endpoints. This service runs on a timer. On run it looks for new files and then shutsdown when completed the upload

## Healthchecks
There are no admin endpoints as the service runs and then shutsdown

