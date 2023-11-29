package controllers

import (
	"Ascend-Developers/xlr8-backend/responses"
	"Ascend-Developers/xlr8-backend/services"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

type AuthController struct {
	Service *services.AuthService
}

func NewAuthController() *AuthController {
	controller := &AuthController{}
	service := &services.AuthService{}
	controller.Service = service
	return controller
}

func (controller *AuthController) Register(ctx *gin.Context) {

	var registerResponse responses.CreateResponse

	// VALIDATE THE USER
	if err := ctx.ShouldBind(&registerResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// GETTING USER BY EMAIL
	if existingUser := controller.Service.FindUserByEmail(registerResponse.Email); existingUser != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "User Email already exists"})
		return
	}

	// HASHING THE PASSWORD
	hashedPassword, err := controller.Service.HashPassword(registerResponse.Password)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	registerResponse.Password = hashedPassword

	// CREATING THE USER
	err = controller.Service.RegisterUser(registerResponse)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Failed to Register User"})
		return
	}

	// Respond with success message
	ctx.JSON(http.StatusOK, gin.H{"message": "User Registered successfully"})
}

func (controller *AuthController) Login(ctx *gin.Context) {

	var loginResponse responses.LoginResponse

	// VALIDATE THE USER
	if err := ctx.ShouldBind(&loginResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// GETTING USER BY EMAIL
	user := controller.Service.FindUserByEmail(loginResponse.Email)
	if user == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Incorrect Email or User Does'nt exists"})
		return
	}

	// VERIFYING THE PASSWORD
	err := controller.Service.VerifyPassword(loginResponse.Password, user.Password)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Incorrect Password"})
		return
	}

	// GENERATING TOKEN
	token, err := controller.Service.GenerateJWT(user.ID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Failed to generate token"})
		return
	}

	// UPDATING THE USER
	err = controller.Service.UpdateUser(user, token)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Error updating User"})
		return
	}

	// RETURNING THE UPDATED USER
	user.Token = token
	ctx.JSON(http.StatusOK, gin.H{"User": user})

}
func (controller *AuthController) ForgetPassword(ctx *gin.Context) {

	email := ctx.Query("email")

	//GETTING THE USER
	user := controller.Service.FindUserByEmail(email)
	if user == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Incorrect Email or User Does'nt exists"})
		return
	}

	// DATE FOR EMAIL
	link := os.Getenv("PASSWORD_LINK")
	data := make(map[string]string)
	data["name"] = user.Name
	data["link"] = link

	//SENDING EMAIL
	emailService := services.EmailService{}
	err := emailService.SendNotifications(email, data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Error Sending Email"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"Success": "Reset Password Link Sent To Your Email"})

}

func (controller *AuthController) ResetPassword(ctx *gin.Context) {

	var changePasswordResponse responses.ResetPasswordResponse

	// VALIDATE THE RESPONSE
	if err := ctx.ShouldBind(&changePasswordResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user := controller.Service.FindUserByEmail(changePasswordResponse.Email)
	if user == nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Incorrect Email or User Does'nt exists"})
		return
	}

	// CHECKING THE NEW PASSWORD
	if changePasswordResponse.NewPassword != changePasswordResponse.ConfirmPassword {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Password Does'nt Match"})
		return
	}

	// HASHING THE NEW PASSWORD
	hashedPassword, err := controller.Service.HashPassword(changePasswordResponse.ConfirmPassword)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// UPDATING THE USER PASSWORD
	err = controller.Service.UpdatePassword(user.ID, hashedPassword)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"Success": "Password Updated"})

}
