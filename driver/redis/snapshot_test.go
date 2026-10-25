package redis

import (
	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/protocol"
	"time"
)

func transitionSnapshot(data []byte, now time.Time, cmd airlock.Command) ([]byte, airlock.Result, error) {
	state, err := protocol.DecodeState(data)
	if err != nil {
		return nil, airlock.Result{}, err
	}
	next, result, _, err := protocol.Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}
	encoded, codecErr := protocol.EncodeState(next)
	if codecErr != nil {
		return nil, airlock.Result{}, codecErr
	}
	return encoded, result, err
}
