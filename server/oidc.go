/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Michael Grace <mg38@sanger.ac.uk>
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/sessions"
	"github.com/joho/godotenv"
	verifier "github.com/okta/okta-jwt-verifier-golang"
	oauthUtils "github.com/okta/okta-jwt-verifier-golang/utils"
	"github.com/thanhpk/randstr"

	"golang.org/x/oauth2"
)

type oauthEnv struct {
	oauth2.Config
	clientRedirect string
}

const oktaCookieName = "okta-hosted-login-session-store"

var sessionStore = sessions.NewCookieStore([]byte("okta-hosted-login-session-store"))

func newOktaOauthConfig(callback string) oauth2.Config {
	return oauth2.Config{
		RedirectURL:  callback,
		ClientID:     os.Getenv("OKTA_OAUTH2_CLIENT_ID"),
		ClientSecret: os.Getenv("OKTA_OAUTH2_CLIENT_SECRET"),
		Scopes:       []string{"openid", "profile", "email"},
		Endpoint: oauth2.Endpoint{
			AuthURL:   os.Getenv("OKTA_OAUTH2_ISSUER") + "/v1/authorize",
			TokenURL:  os.Getenv("OKTA_OAUTH2_ISSUER") + "/v1/token",
			AuthStyle: oauth2.AuthStyleInParams,
		},
	}
}

// TODO comment
func (s *Server) AddOIDCRoutes() {
	// TODO replace with CLI flags
	godotenv.Load("./.okta.env")

	// TODO replace callbacks
	s.webOAuth = &oauthEnv{
		Config:         newOktaOauthConfig("https://172.27.24.73:3000/callback"),
		clientRedirect: "/",
	}

	s.cliOAuth = &oauthEnv{
		Config:         newOktaOauthConfig("https://172.27.24.73:3000/callback-cli"),
		clientRedirect: "/auth-code",
	}

	s.router.GET(EndpointAuthCallback, s.webOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCLogin, s.webOAuth.HandleOIDCLogin)

	s.router.GET(EndpointAuthCLICallback, s.cliOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCCLILogin, s.cliOAuth.HandleOIDCLogin)

	s.router.GET("/auth-code", func(ctx *gin.Context) {
		cookie, err := ctx.Request.Cookie("okta-hosted-login-session-store")
		if err != nil {
			// TODO
		}
		ctx.Writer.Write([]byte(cookie.Value))
	})

}

// TODO comment
func (o *oauthEnv) HandleOIDCCallback(c *gin.Context) {
	session, err := sessionStore.Get(c.Request, "okta-hosted-login-session-store")
	if err != nil {
		c.AbortWithError(http.StatusForbidden, err)
		return
	}

	// Check the state that was returned in the query string is the same as the above state
	if c.Query("state") == "" || c.Query("state") != session.Values["oauth_state"] {
		c.AbortWithError(http.StatusForbidden, fmt.Errorf("the state was not as expected"))
		return
	}

	// Make sure the code was provided
	if c.Query("error") != "" {
		c.AbortWithError(http.StatusForbidden, fmt.Errorf("authorization server returned an error: %s", c.Query("error")))
		return
	}

	// Make sure the code was provided
	if c.Query("code") == "" {
		c.AbortWithError(http.StatusForbidden, fmt.Errorf("the code was not returned or is not accessible"))
		return
	}

	token, err := o.Exchange(
		context.Background(),
		c.Query("code"),
		oauth2.SetAuthURLParam("code_verifier", session.Values["oauth_code_verifier"].(string)),
	)
	if err != nil {
		c.AbortWithError(http.StatusUnauthorized, err)
		return
	}

	// Extract the ID Token from OAuth2 token.
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		c.AbortWithError(http.StatusUnauthorized, fmt.Errorf("id token missing from OAuth2 token"))
		return
	}
	_, err = verifyToken(rawIDToken)

	if err != nil {
		c.AbortWithError(http.StatusForbidden, err)
		return
	} else {
		session.Values["access_token"] = token.AccessToken

		session.Save(c.Request, c.Writer)
	}

	c.Redirect(http.StatusFound, o.clientRedirect)
}

// TODO comment
func (o *oauthEnv) HandleOIDCLogin(c *gin.Context) {
	c.Header("Cache-Control", "no-cache") // See https://github.com/okta/samples-golang/issues/20

	session, err := sessionStore.Get(c.Request, "okta-hosted-login-session-store")
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	// Generate a random state parameter for CSRF security
	oauthState := randstr.Hex(16)

	// Create the PKCE code verifier and code challenge
	oauthCodeVerifier, err := oauthUtils.GenerateCodeVerifierWithLength(50)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	// get sha256 hash of the code verifier
	oauthCodeChallenge := oauthCodeVerifier.CodeChallengeS256()

	session.Values["oauth_state"] = oauthState
	session.Values["oauth_code_verifier"] = oauthCodeVerifier.String()

	session.Save(c.Request, c.Writer)

	redirectURI := o.AuthCodeURL(
		oauthState,
		oauth2.SetAuthURLParam("code_challenge", oauthCodeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)

	c.Redirect(http.StatusFound, redirectURI)
}

func verifyToken(t string) (*verifier.Jwt, error) {
	tv := map[string]string{}
	tv["aud"] = os.Getenv("OKTA_OAUTH2_CLIENT_ID")
	jv := verifier.JwtVerifier{
		Issuer:           os.Getenv("OKTA_OAUTH2_ISSUER"),
		ClaimsToValidate: tv,
	}

	result, err := jv.New().VerifyIdToken(t)
	if err != nil {
		return nil, err
	}

	if result != nil {
		return result, nil
	}

	return nil, fmt.Errorf("token could not be verified")
}

func getProfileData(r *http.Request) (map[string]string, error) {
	m := make(map[string]string)

	session, err := sessionStore.Get(r, "okta-hosted-login-session-store")

	if err != nil || session.Values["access_token"] == nil || session.Values["access_token"] == "" {
		return m, nil
	}

	reqUrl := os.Getenv("OKTA_OAUTH2_ISSUER") + "/v1/userinfo"

	req, err := http.NewRequest("GET", reqUrl, nil)
	if err != nil {
		return m, err
	}

	h := req.Header
	h.Add("Authorization", "Bearer "+session.Values["access_token"].(string))
	h.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return m, err
	}

	json.Unmarshal(body, &m)

	return m, nil
}
