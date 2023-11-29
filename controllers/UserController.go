package controllers

import (
	"Ascend-Developers/xlr8-backend/models"
	"Ascend-Developers/xlr8-backend/responses"
	"Ascend-Developers/xlr8-backend/services"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserController struct {
	Service *services.UserService
}

func NewUserController() *UserController {
	controller := &UserController{}
	service := &services.UserService{}
	controller.Service = service
	return controller
}

func (controller *UserController) Index(ctx *gin.Context) {

	pageSize := ctx.Query("perPage")
	perPage, _ := strconv.Atoi(pageSize)
	if perPage == 0 {
		perPage = 20
	}

	pageNumber := ctx.Query("page")
	page, _ := strconv.Atoi(pageNumber)
	if page == 0 {
		page = 1
	}

	users, total, perPage, err := controller.Service.Paginate(page, perPage, "")
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"data":     users,
		"Page":     page,
		"PerPage":  perPage,
		"Total":    total,
		"LastPage": int(math.Ceil(float64(total) / float64(perPage))),
	})

}

func (controller *UserController) Show(ctx *gin.Context) {

	userId := ctx.Param("id")
	user, err := controller.Service.GetUserById(userId)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"User": user})

}

func (controller *UserController) Profile(ctx *gin.Context) {

	authValue, _ := ctx.Get("Auth")
	authUser, ok := authValue.(*models.User)
	if !ok {
		ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "fail", "error": "Assertion to Type User Failed"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"User": authUser})

}

func (controller *UserController) ProfileUpdate(ctx *gin.Context) {

	authValue, _ := ctx.Get("Auth")
	authUser, ok := authValue.(*models.User)
	if !ok {
		ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "fail", "error": "Assertion to Type User Failed"})
		return
	}

	var profileUpdateResponse responses.ProfileUpdateResponse

	// VALIDATE THE USER
	if err := ctx.ShouldBind(&profileUpdateResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// CHECKING USER EMAIL
	if user := controller.Service.CheckUserEmail(authUser.ID, profileUpdateResponse.Email); user != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "User Email Already Exists"})
		return
	}

	// UPDATING THE USER
	err := controller.Service.UpdateUserProfile(authUser.ID, profileUpdateResponse)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"Success": "User Updated"})

}

func (controller *UserController) Update(ctx *gin.Context) {

	var updateResponse responses.UpdateResponse

	// VALIDATE THE USER
	if err := ctx.ShouldBind(&updateResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userId := ctx.Param("id")
	objectUserID, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// CHECKING USER EMAIL
	if user := controller.Service.CheckUserEmail(objectUserID, updateResponse.Email); user != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "User Email Already Exists"})
		return
	}

	// UPDATING THE USER
	err = controller.Service.UpdateUser(objectUserID, updateResponse)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"Success": "User Updated"})
}

func (controller *UserController) Delete(ctx *gin.Context) {

	userId := ctx.Param("id")

	user, err := controller.Service.GetUserById(userId)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	currentTime := time.Now()
	user.DeletedAt = &currentTime
	user.Deleted = true
	if err = mgm.Coll(user).Update(user); err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "User Deleted successfully"})
}

func (controller *UserController) ChangePassword(ctx *gin.Context) {

	var changePasswordResponse responses.ChangePasswordResponse

	// VALIDATE THE USER
	if err := ctx.ShouldBind(&changePasswordResponse); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// GETTING AUTH USER
	authValue, _ := ctx.Get("Auth")
	authUser, ok := authValue.(*models.User)
	if !ok {
		ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "fail", "error": "Assertion to Type User Failed"})
		return
	}

	// VERIFYING THE CURRENT PASSWORD
	err := controller.Service.VerifyPassword(changePasswordResponse.CurrentPassword, authUser.Password)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Incorrect Password"})
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
	err = controller.Service.UpdatePassword(authUser.ID, hashedPassword)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"Success": "Password Updated"})
}
