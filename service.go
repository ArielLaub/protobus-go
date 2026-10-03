package protobus

import "context"

// Service is a protobus service. (stub)
type Service struct{ name string }

func (s *Service) Name() string                         { return s.name }
func (s *Service) StopConsuming(context.Context) error { return nil }
