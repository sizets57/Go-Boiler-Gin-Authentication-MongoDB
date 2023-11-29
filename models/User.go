package models

import (
	"Ascend-Developers/xlr8-backend/responses"
	"context"
	"time"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type User struct {
	mgm.DefaultModel `bson:",inline"`
	ID               primitive.ObjectID  `bson:"_id,omitempty" json:"_id,omitempty"`
	IDO              *primitive.ObjectID `bson:"id,omitempty" json:"id,omitempty"`
	Name             string              `bson:"name,omitempty" json:"name,omitempty"`
	Email            string              `bson:"email,omitempty" json:"email,omitempty"`
	Gender           string              `bson:"gender,omitempty" json:"gender,omitempty"`
	Phone            string              `bson:"phone,omitempty" json:"phone,omitempty"`
	Company          string              `bson:"company,omitempty" json:"company,omitempty"`
	Field            string              `bson:"field,omitempty" json:"field,omitempty"`
	Password         string              `bson:"password,omitempty" json:"password,omitempty"`
	Token            string              `bson:"token,omitempty" json:"token,omitempty"`
	Type             string              `bson:"type,omitempty" json:"type,omitempty"`
	DeletedAt        *time.Time          `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
	Deleted          bool                `bson:"deleted,omitempty" json:"deleted,omitempty"`
}

func (user *User) NewUser(userResponse responses.CreateResponse) *User {
	return &User{
		Name:     userResponse.Name,
		Email:    userResponse.Email,
		Password: userResponse.Password,
		Type:     userResponse.Type,
		Gender:   userResponse.Gender,
		Phone:    userResponse.Phone,
		Company:  userResponse.Company,
		Field:    userResponse.Field,
	}
}

func (user *User) UpdateById(id primitive.ObjectID, updateField string, updateValue interface{}) error {

	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{updateField: updateValue}}
	_, err := mgm.Coll(&User{}).UpdateOne(mgm.Ctx(), filter, update)

	if err != nil {
		return err
	}
	return nil
}

func (user *User) GetUserById(id string) (*User, error) {

	err := mgm.Coll(user).FindByID(id, user)
	if err != nil {
		return nil, err
	}
	return user, nil

}

func (user *User) GetAllUsers(page int, perPage int, search string) ([]User, int, int, error) {
	users := []User{}

	ctx := mgm.Ctx()
	collection := mgm.Coll(user)
	findOptions := options.Find()

	total, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, 0, 0, err
	}

	findOptions.SetSkip((int64(page) - 1) * int64(perPage))
	findOptions.SetLimit(int64(perPage))
	findOptions.SetSort(bson.M{"created_at": -1})

	cursor, err := collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return nil, 0, 0, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		user := &User{}
		if err := cursor.Decode(user); err != nil {
			return nil, 0, 0, err
		}
		users = append(users, *user)
	}
	return users, int(total), perPage, nil
}

func (user *User) UpdateUser(id primitive.ObjectID, updateValue interface{}) error {

	filter := bson.M{"_id": id}
	_, err := mgm.Coll(user).UpdateOne(context.Background(), filter, bson.M{"$set": updateValue})
	if err != nil {
		return err
	}
	return nil
}

func (user *User) CreateUser() error {

	err := mgm.Coll(user).Create(user)
	if err != nil {
		return err
	}
	return nil
}

func (user *User) GetUserByEmailAndId(id primitive.ObjectID, email string) *User {

	filter := bson.M{"_id": bson.M{"$ne": id}, "email": email}
	err := mgm.Coll(&User{}).First(filter, user)
	if err != nil {
		return nil
	}
	return user
}
