package seqcask_test

import (
    "testing"
    "github.com/stretchr/testify"
)

func TestNewWriteBatch(t *testing.T) {
    batch := seqcask.NewWriteBatch()
    assert.NotNil(t, batch)
}
