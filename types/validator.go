package types

import (
	"encoding/json"
	"github.com/go-playground/validator/v10"
	"sync"
)

var (
	validate *validator.Validate
	once     sync.Once
)

func getValidator() (*validator.Validate, error) {
	var err error
	once.Do(func() {
		validate = validator.New()
		err = validate.RegisterValidation("json", validateJSON)
	})
	return validate, err
}

func validateJSON(fl validator.FieldLevel) bool {
	var js json.RawMessage
	return json.Unmarshal(fl.Field().Bytes(), &js) == nil
}
