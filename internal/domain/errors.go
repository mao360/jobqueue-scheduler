package domain

import "errors"

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrInvalidJob        = errors.New("invalid job")
	ErrJobAlreadyDone    = errors.New("job already in terminal state")
)