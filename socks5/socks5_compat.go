package socks5

func (s *SOCKS5Server) Stop() error { return s.Close() }
