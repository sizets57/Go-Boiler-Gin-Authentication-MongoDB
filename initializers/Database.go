package initializers

import (
	"os"
	"time"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectDb() {
	var err error

	if os.Getenv("MODE") == "DEV" {

		credential := options.Credential{
			AuthMechanism: os.Getenv("AUTH_MECHANISM"),
			AuthSource:    os.Getenv("DB_NAME"),
			Username:      os.Getenv("DB_OWNER"),
			Password:      os.Getenv("DB_PASS"),
		}

		clientOpts := options.Client().ApplyURI(os.Getenv("DB_DSN")).SetAuth(credential)
		err = mgm.SetDefaultConfig(&mgm.Config{CtxTimeout: 30 * time.Second}, os.Getenv("DB_NAME"), clientOpts)
	} else {
		err = mgm.SetDefaultConfig(&mgm.Config{CtxTimeout: 30 * time.Second}, os.Getenv("DB_NAME"), options.Client().ApplyURI(os.Getenv("DB_DSN")))
	}

	if err != nil {
		panic(err)
	}
}

func CloseDb() {

}
