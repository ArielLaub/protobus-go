package gen

import (
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/descriptorpb"
)

func isDeprecated(svc *protogen.Service) bool {
	o, ok := svc.Desc.Options().(*descriptorpb.ServiceOptions)
	return ok && o.GetDeprecated()
}

func isMethodDeprecated(m *protogen.Method) bool {
	o, ok := m.Desc.Options().(*descriptorpb.MethodOptions)
	return ok && o.GetDeprecated()
}
