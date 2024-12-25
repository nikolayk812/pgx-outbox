package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID        uuid.UUID
	Name      string
	Age       int
	CreatedAt time.Time
}

func (u User) String() string {
	return fmt.Sprintf("[name=%s, age=%d, id=%s, created=%s]", u.Name, u.Age, u.ID, u.CreatedAt.Format(time.DateTime))
}

type UserMessagePayload struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	Quote     string    `json:"quote"`
}
