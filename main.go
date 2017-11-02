package main

import (
	"os"

	"errors"
	"strings"

	"strconv"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/loader"
	"github.com/Financial-Times/factset-uploader/rds"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

const appDescription = "Downloads the factset files from Factset SFTP and sends them to S3"

func main() {
	app := cli.App("factset-uploader", appDescription)

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "factset-uploader",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "factset-uploader",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})

	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "info",
		Desc:   "Log level",
		EnvVar: "LOG_LEVEL",
	})

	factsetUser := app.String(cli.StringOpt{
		Name:      "factsetUsername",
		Desc:      "Factset username",
		EnvVar:    "FACTSET_USER",
		HideValue: true,
	})

	factsetKey := app.String(cli.StringOpt{
		Name:      "factsetKey",
		Desc:      "Key to ssh key",
		EnvVar:    "FACTSET_KEY",
		HideValue: true,
	})

	factsetFTP := app.String(cli.StringOpt{
		Name:   "factsetFTP",
		Value:  "fts-sftp.factset.com",
		Desc:   "factset ftp server address",
		EnvVar: "FACTSET_FTP",
	})

	factsetPort := app.Int(cli.IntOpt{
		Name:   "factsetPort",
		Value:  6671,
		Desc:   "Factset connection port",
		EnvVar: "FACTSET_PORT",
	})

	packages := app.String(cli.StringOpt{
		Name:   "packages",
		Value:  "",
		Desc:   "List of packages to process (dataset,package,product,feedVersion) separated by a semicolon",
		EnvVar: "PACKAGES",
	})

	workspace := app.String(cli.StringOpt{
		Name:   "workspace",
		Value:  "/vol/factset",
		Desc:   "Location to be used to download and process files from, should end in 'factset'. This directory will be cleared down and recreated on application start so be very careful",
		EnvVar: "WORKSPACE",
	})

	rdsDSN := app.String(cli.StringOpt{
		Name:      "rdsDSN",
		Desc:      "DSN to connect to the RDS e.g. user:pass@host/schema - it should not contain any parameters",
		EnvVar:    "RDS_DSN",
		HideValue: true,
	})

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"logLevel": *logLevel}).Fatal("Cannot parse log level")
	}
	log.SetLevel(lvl)
	log.SetFormatter(&log.JSONFormatter{})

	log.WithFields(log.Fields{
		"APP_SYSTEM_CODE": *appSystemCode,
		"LOG_LEVEL":       *logLevel,
		"FACTSET_FTP":     *factsetFTP,
	}).Infof("[Startup] %v is starting", *appName)

	app.Action = func() {
		splitConfig := strings.Split(*workspace, "/")
		if splitConfig[len(splitConfig)-1] != "factset" {
			log.Fatal("Specified workspace is not valid as highest level folder is not 'factset'")
			return
		}
		factsetService, err := factset.NewService(*factsetUser, *factsetKey, *factsetFTP, *factsetPort, *workspace)
		if err != nil {
			log.Fatal(err)
			return
		}

		rdsService, err := rds.NewClient(*rdsDSN)
		if err != nil {
			log.Fatal(err)
			return
		}

		config, err := convertConfig(*packages)
		if err != nil {
			log.Fatal(err)
			return
		}

		factsetLoader := loader.NewService(config, rdsService, factsetService, *workspace)
		factsetLoader.LoadPackages()
		return
	}

	err = app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func convertConfig(configString string) (loader.Config, error) {

	var config loader.Config
	splitConfig := strings.Split(configString, ";")
	for _, pkg := range splitConfig {
		splitPkg := strings.Split(pkg, ",")
		if len(splitPkg) != 4 {
			return loader.Config{}, errors.New("Package config is incorrectly configured; it has the wrong number of values. See readme for instructions")
		}

		version, _ := strconv.Atoi(splitPkg[3])
		config.AddPackage(factset.Package{
			Dataset:     splitPkg[0],
			FSPackage:   splitPkg[1],
			Product:     splitPkg[2],
			FeedVersion: version,
		})
	}

	return config, nil
}
