package types

import "fmt"

type ForwardStats struct {
	Read      int
	Published int
	Acked     int
}

func (fs ForwardStats) String() string {
	return fmt.Sprintf("[read: %d, published: %d, acked: %d]", fs.Read, fs.Published, fs.Acked)
}
