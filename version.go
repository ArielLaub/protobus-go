package protobus

import "github.com/ArielLaub/protobus-go/v2/internal/version"

// Version is the protobus-go release, reported to the broker as a client
// property so operators can see which port and version each connection runs.
const Version = version.Version

// SupportPackageIsVersion1 is referenced by generated code. Generated files
// reference the constant for the generated-code contract they were written
// for; a library that no longer supports that contract does not define it,
// so stale generated code fails to compile with an error naming the cause
// rather than misbehaving.
const SupportPackageIsVersion1 = true
