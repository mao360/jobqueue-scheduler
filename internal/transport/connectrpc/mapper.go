package connectrpc

import (
	"errors"

	"connectrpc.com/connect"
	"github.com/mao360/jobqueue-scheduler/internal/domain"
)

func toConnectErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrJobNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, domain.ErrInvalidTransition):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, domain.ErrInvalidJob):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, domain.ErrJobAlreadyDone):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, domain.ErrJobAlreadyExists):
		return connect.NewError(connect.CodeAlreadyExists, err)
	default:
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
}
