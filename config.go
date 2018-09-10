package sqslib

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ConfigLoader interface {
	LoadConfig(name string, dest interface{}) error
}

type DefaultConfigLoader struct {
	prefix string
	logger *zap.SugaredLogger
}

func (d *DefaultConfigLoader) LoadConfig(name string, target interface{}) error {
	fullPrefix := d.prefix + "_" + name
	d.logger.Debugw("Loading config with prefix", "prefix", fullPrefix)
	if err := envconfig.Process(fullPrefix, target); err != nil {
		return errors.Wrapf(err, "loading config from environment for %s", fullPrefix)
	}
	d.logger.Debugw("Loaded config", "config", target)
	return nil
}
