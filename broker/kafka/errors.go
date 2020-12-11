package kafka

import "errors"

var (
	// ErrMissingEncoder report the encoder is missing.
	ErrMissingEncoder = errors.New("kafka: missing encoder")
)
