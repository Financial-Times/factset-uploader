# factset-uploader

[![Circle CI](https://circleci.com/gh/Financial-Times/factset-uploader/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/factset-uploader/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/factset-uploader)](https://goreportcard.com/report/github.com/Financial-Times/factset-uploader) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/factset-uploader/badge.svg)](https://coveralls.io/github/Financial-Times/factset-uploader)

## Introduction

Downloads the factset files for orgs, people, financial instruments and fundamentals from Factset SFTP and sends them to S3.

The service is run on a timer. It starts and then it checks for any new factset files. It extracts the files from the zips and then sends them to S3. Once complete the service shutdowns.

Currently there are several categories of files from factset that are of interest to us:

1. Entity information that is used for our organisation information
2. People information that is used for our people and membership information
3. Reference information to provide on things such as controlled vocabulary i.e Role names
4. Symbology information to provide other indentifiers such as the financial instruments
5. Fundamental information to provide information on the financials for entities

## Installation
Download the source code, dependencies and test dependencies:

        go get -u github.com/kardianos/govendor
        go get -u github.com/Financial-Times/factset-uploader
        cd $GOPATH/src/github.com/Financial-Times/factset-uploader
        govendor sync
        go build .

## Running locally
_How can I run it_

_TODO: How do we run this locally with the whitelist on the Factset side, tunnel?_

1. Install MySql
    brew info mysql56
    OSX:
    ```
    brew install mysql56
    echo 'export PATH="/usr/local/opt/mysql@5.6/bin:$PATH"' >> ~/.bash_profile
    ```

    Windows:
    
        https://dev.mysql.com/downloads/mysql/5.6.html#downloads


2. Install MySql Workbench (Optional)

    https://dev.mysql.com/downloads/workbench/

3. Run the tests and install the binary:

        govendor sync
        govendor test -v -race
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/factset-uploader [--help]

Options:

        --app-system-code="factset-uploader"   System Code of the application ($APP_SYSTEM_CODE)
        --app-name="factset-uploader"               Application name ($APP_NAME)
        --awsAccessKey=xxx
        --awsSecretKey=xxx
        --bucketName=com.ft.coco-factset-data
        --factsetUser=xxx
        --factsetKey=xxx
        --factsetFTP=fts-sftp.factset.com
        --factsetPort=6671
        --resources=/directory/without/version:zip_or_txt_file_to_download

The resources argument specifies a comma separated list of archives and files within that archive to be downloaded from Factset FTP server.
        
3. Test:

    1. _How should we run this locally_

## Build and deployment
* Built by Docker Hub on merge to master: [coco/factset-uploader](https://hub.docker.com/r/coco/factset-uploader/)
* CI provided by CircleCI: [factset-uploader](https://circleci.com/gh/Financial-Times/factset-uploader)

## Service endpoints
There are no service endpoints. This service runs on a timer. On run it looks for new files and then shutsdown when completed the upload

## Healthchecks
There are no admin endpoints as the service runs and then shutsdown

