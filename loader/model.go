package loader

import "github.com/Financial-Times/factset-uploader/factset"

// Config - Which packages to load
type Config struct {
	packages []factset.Package
}

// AddPackage - append new package
func (c *Config) AddPackage(p factset.Package) {
	c.packages = append(c.packages, p)
}
