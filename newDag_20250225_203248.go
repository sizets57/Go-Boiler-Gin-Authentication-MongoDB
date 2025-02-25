package main

import (
	"Ascend-Developers/xlr8-backend/initializers"
	"Ascend-Developers/xlr8-backend/router"
	"os"

	"github.com/gin-gonic/gin"
)

func init() {
	initializers.LoadEnv()
	initializers.ConnectDb()

}

func main() {
	//SetUp App
	app := gin.Default()

	//SetUp Routes
	router.Cors(app)
	router.ApiRoutes(app)

	//Start App
	app.Run(os.Getenv("PORT"))

}
