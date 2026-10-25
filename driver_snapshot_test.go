package airlock

import "time"

func transitionSnapshot(data []byte, now time.Time, cmd Command) ([]byte, Result, error) {
	state, err := DecodeState(data)
	if err != nil {
		return nil, Result{}, err
	}
	next, result, _, err := Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}
	encoded, codecErr := EncodeState(next)
	if codecErr != nil {
		return nil, Result{}, codecErr
	}
	return encoded, result, err
}
func transitionStructured(state *DriverState, now time.Time, cmd Command) (*DriverState, Result, error) {
	next, result, _, err := Transition(state, now, cmd)
	return next, result, err
}
func transitionOptimistic(data []byte, now time.Time, cmd Command) ([]byte, Result, CommitWindow, error) {
	state, err := DecodeState(data)
	if err != nil {
		return nil, Result{}, CommitWindow{}, err
	}
	next, result, window, err := Transition(state, now, cmd)
	if next == nil {
		return nil, result, window, err
	}
	encoded, codecErr := EncodeState(next)
	if codecErr != nil {
		return nil, Result{}, CommitWindow{}, codecErr
	}
	return encoded, result, window, err
}
