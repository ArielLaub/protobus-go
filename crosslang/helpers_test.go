package crosslang_test

import (
	"math/big"

	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

type interopBigint = pbtypes.Bigint

func newBigint(v *big.Int) (*pbtypes.Bigint, error) { return pbtypes.NewBigint(v) }
