package services

import (
	"Ascend-Developers/xlr8-backend/models"
	"Ascend-Developers/xlr8-backend/responses"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/crypto/bcrypt"
)

type UserService struct {
}

func (service *UserService) Paginate(page int, pageSize int, search string) ([]models.User, int, int, error) {

	// Calculate the offset based on the page number and page size
	user := &models.User{}
	users, total, perPage, err := user.GetAllUsers(page, pageSize, search)
	if err != nil {
		return nil, 0, 0, err
	}
	return users, total, perPage, nil
}

func (service *UserService) GetUserById(userId string) (*models.User, error) {

	user := &models.User{}
	user, err := user.GetUserById(userId)
	if err != nil {
		return nil, err
	}
	return user, nil
}

// hashPassword hashes the given password using bcrypt
func (service *UserService) HashPassword(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedPassword), nil
}

func (service *UserService) UpdateUser(id primitive.ObjectID, updateResponse responses.UpdateResponse) error {

	user := &models.User{}
	err := user.UpdateUser(id, updateResponse)
	if err != nil {
		return err
	}
	return nil
}

func (service *UserService) UpdateUserProfile(id primitive.ObjectID, profileUpdateResponse responses.ProfileUpdateResponse) error {

	user := &models.User{}
	err := user.UpdateUser(id, profileUpdateResponse)
	if err != nil {
		return err
	}
	return nil
}

func (service *UserService) CheckUserEmail(userId primitive.ObjectID, email string) *models.User {

	user := &models.User{}
	user = user.GetUserByEmailAndId(userId, email)
	return user
}

func (service *UserService) VerifyPassword(userPassword string, providedPassword string) error {
	err := bcrypt.CompareHashAndPassword([]byte(providedPassword), []byte(userPassword))

	if err != nil {
		return err
	}
	return nil
}

func (service *UserService) UpdatePassword(id primitive.ObjectID, newPassword string) error {

	user := &models.User{}
	err := user.UpdateById(id, "password", newPassword)
	if err != nil {
		return err
	}
	return nil
}
