package redis

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tr1v3r/airlock"
)

const inspectScript = `
	local meta = redis.call('HGET', KEYS[1], 'meta')
	local exists = redis.call('EXISTS', KEYS[1])
	local now = redis.call('TIME')
	return {meta or '', exists, now[1], now[2]}
`

// Resize uses the ordinary owner/time-guarded update path for one resource.
func (d *Driver) Resize(ctx context.Context, request airlock.ResizeRequest) (airlock.ResourceStatus, error) {
	r, err := d.execute(ctx, airlock.Command{Kind: airlock.Resize, Resize: request})
	if err != nil {
		return airlock.ResourceStatus{}, err
	}
	return r.ResourceStatus, nil
}

// InspectResource reads only the committed resource header; it takes no update
// lock and performs no expiry maintenance. Counts describe the saved StateTime.
func (d *Driver) InspectResource(ctx context.Context, resource string) (airlock.ResourceStatus, error) {
	if err := ctx.Err(); err != nil {
		return airlock.ResourceStatus{}, err
	}
	if err := airlock.ValidateResource(resource); err != nil {
		return airlock.ResourceStatus{}, err
	}
	if d == nil || d.client == nil {
		return airlock.ResourceStatus{}, errors.New("airlock/redis: uninitialized driver")
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	raw, err := d.eval(ctx, inspectScript, d.key(resource))
	if err != nil {
		return airlock.ResourceStatus{}, err
	}
	values, ok := raw.([]any)
	if !ok || len(values) != 4 {
		return airlock.ResourceStatus{}, airlock.ErrCorruptState
	}
	_, now, err := decodeRead([]any{int64(0), "", values[2], values[3]})
	if err != nil {
		return airlock.ResourceStatus{}, err
	}
	meta, ok := values[0].(string)
	exists, valid := values[1].(int64)
	if !ok || !valid || (exists != 0 && exists != 1) {
		return airlock.ResourceStatus{}, airlock.ErrCorruptState
	}
	if exists == 0 {
		return airlock.ResourceStatus{}, airlock.ErrResourceNotFound
	}
	var header observationHeader
	if json.Unmarshal([]byte(meta), &header) != nil || header.Token == "" {
		return airlock.ResourceStatus{}, airlock.ErrCorruptState
	}
	if err := ctx.Err(); err != nil {
		return airlock.ResourceStatus{}, err
	}
	return header.Inspect(now, resource)
}

var _ airlock.ResourceManager = (*Driver)(nil)
