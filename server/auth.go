/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package server

import (
	"net/http"
	"strings"
	"time"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
)

type login struct {
	Username string `form:"username" json:"username" binding:"required"`
	Password string `form:"password" json:"password" binding:"required"`
}

const (
	tokenDuration    = time.Hour * 24 * 7
	userKey          = "user"
	claimKeyUsername = "Username"
	claimKeyUIDs     = "UIDs"
	claimKeyGIDs     = "GIDs"
	ErrBadJWTClaim   = Error("JWT had bad claims")
)

// User is what we store in our JWTs.
type User struct {
	Username string
	UIDs     []string
	GIDs     []string
}

// EnableAuth adds the /rest/v1/jwt POST and GET endpoints to the REST API.
//
// The /rest/v1/jwt POST endpoint requires the username and password parameters
// in a form or as JSON.
// It passes these to the given callback, and if it returns true, a JWT is
// returned (as a JSON string) in the response that contains Username,
// UIDs and GIDs information (the latter 2 being comma separated strings).
//
// Queries to endpoints that need authorisation should include the JWT in the
// authorization header as a bearer token. Those endpoints can be implemented
// by extracting the *User information out of the JWT using getUser().
//
// JWTs are signed and verified using the given cert and key files.
//
// GET on the endpoint will refresh the JWT.
func (s *Server) EnableAuth(certFile, keyFile string, cb AuthCallback) error {
	s.authCB = cb

	authMiddleware, err := s.createAuthMiddleware(certFile, keyFile)
	if err != nil {
		return err
	}

	s.router.POST(EndPointJWT, authMiddleware.LoginHandler)
	s.router.GET(EndPointJWT, authMiddleware.RefreshHandler)

	auth := s.router.Group(EndPointAuth)
	auth.Use(authMiddleware.MiddlewareFunc())
	s.authGroup = auth

	return nil
}

// createAuthMiddleware creates jin-compatible middleware that enables logins
// and authorisation with JWTs.
func (s *Server) createAuthMiddleware(certFile, keyFile string) (*jwt.GinJWTMiddleware, error) {
	return jwt.New(&jwt.GinJWTMiddleware{
		Realm:            "wrstat",
		SigningAlgorithm: "RS512",
		PubKeyFile:       certFile,
		PrivKeyFile:      keyFile,
		Timeout:          tokenDuration,
		MaxRefresh:       tokenDuration,
		IdentityKey:      userKey,
		PayloadFunc:      authPayLoad,
		IdentityHandler:  authIdentityHandler,
		Authenticator:    s.authenticator,
		Authorizator: func(data interface{}, c *gin.Context) bool {
			return data != nil
		},
		LoginResponse: func(c *gin.Context, code int, token string, t time.Time) {
			c.JSON(http.StatusOK, token)
		},
		TokenLookup:   "header: Authorization",
		TokenHeadName: "Bearer",
		TimeFunc:      time.Now,
	})
}

// authPayLoad is a function property for jwt.GinJWTMiddleware. It adds extra
// claims to the JWT we send to the user.
func authPayLoad(data interface{}) jwt.MapClaims {
	if v, ok := data.(*User); ok {
		return jwt.MapClaims{
			claimKeyUsername: v.Username,
			claimKeyUIDs:     strings.Join(v.UIDs, ","),
			claimKeyGIDs:     strings.Join(v.GIDs, ","),
		}
	}

	return jwt.MapClaims{}
}

// authIdentityHandler is a function property for jwt.GinJWTMiddleware. It
// extracts their user-related claims we stored in the JWT and turns them into
// a *User.
func authIdentityHandler(c *gin.Context) interface{} {
	claims := jwt.ExtractClaims(c)

	username, err1 := retrieveClaimString(claims, claimKeyUsername)
	uids, err2 := retrieveClaimString(claims, claimKeyUIDs)
	gids, err3 := retrieveClaimString(claims, claimKeyGIDs)

	if username == "" || err1 != nil || err2 != nil || err3 != nil {
		return nil
	}

	return &User{
		Username: username,
		UIDs:     strings.Split(uids, ","),
		GIDs:     strings.Split(gids, ","),
	}
}

// retrieveClaimString finds and converts to a string the given claim in amongst
// the given claims. If it doesn't exist or convert to a string, returns an
// error.
func retrieveClaimString(claims jwt.MapClaims, claim string) (string, error) {
	value, existed := claims[claim]
	if !existed {
		return "", ErrBadJWTClaim
	}

	str, ok := value.(string)
	if !ok {
		return "", ErrBadJWTClaim
	}

	return str, nil
}

// authenticator is a function property for jwt.GinJWTMiddleware. It gets the
// username and password from the query, passes them to our authCB, and creates
// a *User on success. That in turn gets passed to authPayload().
func (s *Server) authenticator(c *gin.Context) (interface{}, error) {
	var loginVals login
	if err := c.ShouldBind(&loginVals); err != nil {
		return nil, jwt.ErrMissingLoginValues
	}

	username := loginVals.Username
	password := loginVals.Password

	ok, uids, gids := s.authCB(username, password)

	if !ok {
		return nil, jwt.ErrFailedAuthentication
	}

	return &User{
		Username: username,
		UIDs:     uids,
		GIDs:     gids,
	}, nil
}

// getUser retreives the *User information extracted from the JWT in the auth
// header. This will only be present after calling EnableAuth(), on a route in
// the authGroup.
func (s *Server) getUser(c *gin.Context) *User {
	userI, ok := c.Get(userKey)
	if !ok {
		return nil
	}

	user, ok := userI.(*User)
	if !ok {
		return nil
	}

	return user
}
