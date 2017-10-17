package main

import (
	"os"

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
		Value:  "INFO",
		Desc:   "Log level",
		EnvVar: "LOG_LEVEL",
	})

	factsetUser := app.String(cli.StringOpt{
		Name:   "factsetUsername",
		Desc:   "Factset username",
		EnvVar: "FACTSET_USER",
	})

	factsetKey := app.String(cli.StringOpt{
		Name:   "factsetKey",
		Desc:   "Key to ssh key",
		EnvVar: "FACTSET_KEY",
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

	resources := app.String(cli.StringOpt{
		Name:   "factsetResources",
		Value:  "",
		Desc:   "factset resources to be loaded",
		EnvVar: "FACTSET_RESOURCES",
	})

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"logLevel": *logLevel}).Fatal("Cannot parse log level")
	}
	log.SetLevel(lvl)
	log.SetFormatter(&log.JSONFormatter{})

	log.WithFields(log.Fields{
		"APP_SYSTEM_CODE":   *appSystemCode,
		"LOG_LEVEL":         *logLevel,
		"FACTSET_FTP":       *factsetFTP,
		"FACTSET_RESOURCES": *resources,
	}).Infof("[Startup] %v is starting", *appName)

	app.Command("run", "Runs the uploader", func(app *cli.Cmd) {
		app.Action = func() {

			//factsetLoader := loader.NewService()
		}
	})

	runService(factsetUser, factsetKey, factsetFTP, factsetPort, resources)
	err = app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func runService(factsetUser *string, factsetKey *string, factsetFTP *string, factsetPort *int, resources *string) {
	// Do something with all of this
	return
}
