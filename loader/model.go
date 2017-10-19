package loader

import "github.com/Financial-Times/factset-uploader/factset"

type Config struct {
	packages []factset.Package
}

func (c *Config) AddPackage(p factset.Package) {
	c.packages = append(c.packages, p)
}
