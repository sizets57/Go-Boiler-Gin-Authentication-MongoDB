package router

import (
	"Ascend-Developers/xlr8-backend/controllers"
	"Ascend-Developers/xlr8-backend/middlewares"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func Cors(r *gin.Engine) {

	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		// AllowOrigins:     []string{"https://nhcc.ascend.com.sa"},
		AllowMethods:     []string{"PUT", "PATCH", "GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization", "Accept"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))
}

func ApiRoutes(r *gin.Engine) {

	router := r.Group("/api")
	router.POST("/ping", func(context *gin.Context) {
		context.JSON(http.StatusOK, gin.H{"status": "success", "data": "working...", "version": "version"})

	})

	// AUTH ROUTES
	authController := controllers.NewAuthController()
	router.POST("/register", authController.Register)
	router.POST("/login", authController.Login)
	router.POST("/forgetPassword", authController.ForgetPassword)

	// USER ROUTES
	userRoutes := router.Group("/users")
	userRoutes.Use(middlewares.Auth())

	userController := controllers.NewUserController()
	userRoutes.GET("", userController.Index)
	userRoutes.GET("/show/:id", userController.Show)
	userRoutes.POST("/update/:id", userController.Update)
	userRoutes.POST("/delete/:id", userController.Delete)
	userRoutes.GET("/profile", userController.Profile)
	userRoutes.POST("/profile/update", userController.ProfileUpdate)
	userRoutes.POST("/changePassword", userController.ChangePassword)

}
