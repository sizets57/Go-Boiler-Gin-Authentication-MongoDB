package responses

type LoginResponse struct {
	Email    string `bson:"email" json:"email"  binding:"required,email"`
	Password string `bson:"password" json:"password"  binding:"required"`
}

type CreateResponse struct {
	Name     string `bson:"name" json:"name" binding:"required"`
	Type     string `bson:"type" json:"type" binding:"required"`
	Email    string `bson:"email" json:"email" binding:"required,email"`
	Password string `bson:"password" json:"password" binding:"required"`
	Gender   string `bson:"gender" json:"gender" binding:"required"`
	Phone    string `bson:"phone" json:"phone" binding:"required"`
	Company  string `bson:"company" json:"company"`
	Field    string `bson:"field" json:"field"`
}

type UpdateResponse struct {
	Name    string `bson:"name" json:"name" binding:"required"`
	Email   string `bson:"email" json:"email" binding:"required,email"`
	Gender  string `bson:"gender" json:"gender" binding:"required"`
	Phone   string `bson:"phone" json:"phone" binding:"required"`
	Company string `bson:"company" json:"company"`
	Field   string `bson:"field" json:"field"`
}

type ProfileUpdateResponse struct {
	Name  string `bson:"name" json:"name" binding:"required"`
	Email string `bson:"email" json:"email" binding:"required,email"`
	Phone string `bson:"phone" json:"phone" binding:"required"`
}

type ChangePasswordResponse struct {
	CurrentPassword string `bson:"currentPassword" json:"currentPassword" binding:"required"`
	NewPassword     string `bson:"newPassword" json:"newPassword" binding:"required"`
	ConfirmPassword string `bson:"confirmPassword" json:"confirmPassword" binding:"required"`
}

type ResetPasswordResponse struct {
	Email           string `bson:"email" json:"email" binding:"required,email"`
	NewPassword     string `bson:"newPassword" json:"newPassword" binding:"required"`
	ConfirmPassword string `bson:"confirmPassword" json:"confirmPassword" binding:"required"`
}
