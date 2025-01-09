package types

import "fmt"

type ForwardOutput struct {
	Read         Messages
	PublishedIDs []int64
	AckedIDs     []int64
}

func (fs ForwardOutput) String() string {
	return fmt.Sprintf("[read: %d, published: %d, acked: %d]", len(fs.Read), len(fs.PublishedIDs), len(fs.AckedIDs))
}
