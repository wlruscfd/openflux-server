package transport

type StripedRoom struct {
	*StripedTransport
	inner Transport
}

func NewStripedRoom(inner Transport) *StripedRoom {
	return &StripedRoom{StripedTransport: NewStripedTransport([]Transport{inner}), inner: inner}
}

func (s *StripedRoom) RoomList() string {
	if r, ok := s.inner.(interface{ RoomList() string }); ok {
		return r.RoomList()
	}
	return ""
}

func (s *StripedRoom) OnRoomList(f func(string)) {
	if r, ok := s.inner.(interface{ OnRoomList(func(string)) }); ok {
		r.OnRoomList(f)
	}
}

func (s *StripedRoom) ProvideCookies(raw string) {
	if p, ok := s.inner.(interface{ ProvideCookies(string) }); ok {
		p.ProvideCookies(raw)
	}
}

func (s *StripedRoom) FetchCookies() (map[string]string, error) {
	if ce, ok := s.inner.(CookieExchanger); ok {
		return ce.FetchCookies()
	}
	return nil, nil
}

func (s *StripedRoom) ApplyCookies(v map[string]string) error {
	if ce, ok := s.inner.(CookieExchanger); ok {
		return ce.ApplyCookies(v)
	}
	return nil
}
