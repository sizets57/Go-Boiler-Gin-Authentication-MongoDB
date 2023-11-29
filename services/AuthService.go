package services

import (
	"Ascend-Developers/xlr8-backend/models"
	"Ascend-Developers/xlr8-backend/responses"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
}

func (service *AuthService) RegisterUser(registerResponse responses.CreateResponse) error {

	user := &models.User{}
	user = user.NewUser(registerResponse)
	err := user.CreateUser()
	if err != nil {
		return err
	}
	return nil
}

func (service *AuthService) UpdateUser(user *models.User, token string) error {
	if err := user.UpdateById(user.ID, "token", token); err != nil {
		return err
	}
	return nil
}

func (service *AuthService) FindUserByEmail(email string) *models.User {
	user := &models.User{}
	if err := mgm.Coll(user).First(bson.M{"email": email}, user); err != nil {
		return nil
	}
	return user
}

// hashPassword hashes the given password using bcrypt
func (service *AuthService) HashPassword(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedPassword), nil
}

func (service *AuthService) VerifyPassword(userPassword string, providedPassword string) error {
	err := bcrypt.CompareHashAndPassword([]byte(providedPassword), []byte(userPassword))

	if err != nil {
		return err
	}
	return nil
}

func (service *AuthService) GenerateJWT(userID primitive.ObjectID) (string, error) {

	// Define the JWT claims, including the email and expiration time
	claims := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": userID,
		"exp":     time.Now().Add(time.Hour * 24 * 365).Unix(), // Token expires in 1 year
	})

	// Replace "your-secret-key" with your actual secret key
	token, err := claims.SignedString([]byte(os.Getenv("SECRET_KEY")))
	if err != nil {
		return "", err
	}

	return token, nil
}

func (service *AuthService) UpdatePassword(id primitive.ObjectID, newPassword string) error {

	user := &models.User{}
	err := user.UpdateById(id, "password", newPassword)
	if err != nil {
		return err
	}
	return nil
}
