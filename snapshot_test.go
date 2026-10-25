package airlock

import (
	"github.com/tr1v3r/airlock/protocol"
	"time"
)

func transitionSnapshot(data []byte, now time.Time, cmd Command) ([]byte, Result, error) {
	state, err := protocol.DecodeState(data)
	if err != nil {
		return nil, Result{}, err
	}
	next, result, _, err := protocol.Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}
	encoded, codecErr := protocol.EncodeState(next)
	if codecErr != nil {
		return nil, Result{}, codecErr
	}
	return encoded, result, err
}

type durableSnapshot = protocol.State
