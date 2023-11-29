package middlewares

import (
	"Ascend-Developers/xlr8-backend/models"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
)

func Auth() gin.HandlerFunc {
	return func(ctx *gin.Context) {

		var token string
		authorizationHeader := ctx.Request.Header.Get("Authorization")

		fields := strings.Fields(authorizationHeader)

		if len(fields) != 0 && fields[0] == "Bearer" {
			token = fields[1]
		}

		if token == "" {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "fail", "error": "UnAuthorized"})
			return
		}

		user := &models.User{}
		_ = mgm.Coll(user).First(bson.M{
			"token": token,
			"$or": []bson.M{
				{"deleted": bson.M{"$exists": false}},
				{"deleted": false},
			}}, user)

		if user.Name == "" {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"status": "fail", "error": "UnAuthorized"})
			return
		}
		ctx.Set("Auth", user)
		ctx.Next()
	}
}
