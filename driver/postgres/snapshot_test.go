package postgres

import (
	"github.com/tr1v3r/airlock"
	"time"
)

func transitionSnapshot(data []byte, now time.Time, cmd airlock.Command) ([]byte, airlock.Result, error) {
	state, err := airlock.DecodeState(data)
	if err != nil {
		return nil, airlock.Result{}, err
	}
	next, result, _, err := airlock.Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}
	encoded, codecErr := airlock.EncodeState(next)
	if codecErr != nil {
		return nil, airlock.Result{}, codecErr
	}
	return encoded, result, err
}
