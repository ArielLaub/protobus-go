package protobus

// Hooks for the black-box tests in package protobus_test.

// WithDialerForTest substitutes the transport.
var WithDialerForTest = withDialer
