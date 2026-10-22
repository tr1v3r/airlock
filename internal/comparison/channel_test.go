package comparison

import "context"

// channelBaseline preserves the bounded channel strategy from the old Pool for
// experiments only. It is not an exported or unlimited-mode compatibility API.
type channelBaseline struct{ tokens chan struct{} }

func newChannelBaseline(capacity int) *channelBaseline {
	return &channelBaseline{tokens: make(chan struct{}, capacity)}
}

func (p *channelBaseline) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.tokens <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *channelBaseline) Release() {
	select {
	case <-p.tokens:
	default:
		panic("release without acquisition")
	}
}
