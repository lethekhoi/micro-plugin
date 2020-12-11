package kafka

import (
	"github.com/pthethanh/micro/broker"
	"github.com/pthethanh/micro/log"
)

// Encoder is an option to provide a custom encoder.
func Encoder(encoder broker.Encoder) Option {
	return func(opts *Kafka) {
		opts.encoder = encoder
	}
}

// Address is an option to set target addresses of Kafka server.
// Multiple addresses are separated by comma.
func Address(addrs string) Option {
	return func(opts *Kafka) {
		opts.addrs = addrs
	}
}

// Logger is an option to provide custom logger.
func Logger(logger log.Logger) Option {
	return func(opts *Kafka) {
		opts.log = logger
	}
}
