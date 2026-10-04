package transport

func (c *CompressedTransport) SetEventCallback(fn func(code, detail string)) {
	SetEventCallback(c.Transport, fn)
}

func (c *CompressedTransport) ForceReconnect() { ForceReconnect(c.Transport) }

func (b *BatchedTransport) SetEventCallback(fn func(code, detail string)) {
	SetEventCallback(b.Transport, fn)
}

func (b *BatchedTransport) ForceReconnect() { ForceReconnect(b.Transport) }

func (e *TokenEncryptedTransport) SetEventCallback(fn func(code, detail string)) {
	SetEventCallback(e.Transport, fn)
}

func (e *TokenEncryptedTransport) ForceReconnect() { ForceReconnect(e.Transport) }
