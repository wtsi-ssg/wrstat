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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/sessions"
	verifier "github.com/okta/okta-jwt-verifier-golang"
	oauthUtils "github.com/okta/okta-jwt-verifier-golang/utils"
	"github.com/thanhpk/randstr"
	"golang.org/x/oauth2"
)

const (
	oktaCookieName = "okta-hosted-login-session-store"

	ErrCouldNotVerifyToken = Error("token could not be verified")
	ErrOIDCUnexpectedState = Error("the state was not as expected")
	ErrOIDCUnavailableCode = Error("the code was not returned or is not accessible")
	ErrOIDCMissingToken    = Error("id token missing from OAuth2 token")
	ErrJSONValueNotString  = Error("non-string value in JSON field")
)

type oAuthParameters struct {
	issuer       string
	clientID     string
	clientSecret string
}

type oauthEnv struct {
	oauth2.Config
	params         oAuthParameters
	sessionStore   *sessions.CookieStore
	clientRedirect string
}

// newOauthEnv returns a oauthEnv with the appropriate values based
// on defaults, the paramenters in oAuthParameters and the callack URL.
func (p oAuthParameters) newOauthEnv(callback, clientRedirect string) *oauthEnv {
	return &oauthEnv{
		Config: oauth2.Config{
			RedirectURL:  callback,
			ClientID:     p.clientID,
			ClientSecret: p.clientSecret,
			Scopes:       []string{"openid", "profile", "email"},
			Endpoint: oauth2.Endpoint{
				AuthURL:   p.issuer + "/v1/authorize",
				TokenURL:  p.issuer + "/v1/token",
				AuthStyle: oauth2.AuthStyleInParams,
			},
		},
		params:         p,
		sessionStore:   sessions.NewCookieStore([]byte(oktaCookieName)),
		clientRedirect: clientRedirect,
	}
}

func cliAuthCode(ctx *gin.Context) {
	cookie, err := ctx.Request.Cookie(oktaCookieName)
	if err != nil {
		ctx.Writer.Write([]byte(err.Error())) //nolint:errcheck

		return
	}

	ctx.Writer.Write([]byte(cookie.Value)) //nolint:errcheck
}

// AddOIDCRoutes creates the OAuth environments for both the web app and the CLI
// and adds the login and callback endpoints, along with an endpoint to get an
// auth code for the CLI.
func (s *Server) AddOIDCRoutes(issuer, clientID, clientSecret string) {
	params := oAuthParameters{
		issuer:       issuer,
		clientID:     clientID,
		clientSecret: clientSecret,
	}

	s.webOAuth = params.newOauthEnv(s.Address+EndpointAuthCallback, "/")
	s.cliOAuth = params.newOauthEnv(s.Address+EndpointAuthCLICallback, EndpointCLIAuthCode)

	s.router.GET(EndpointAuthCallback, s.webOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCLogin, s.webOAuth.HandleOIDCLogin)

	s.router.GET(EndpointAuthCLICallback, s.cliOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCCLILogin, s.cliOAuth.HandleOIDCLogin)

	s.router.GET(EndpointCLIAuthCode, cliAuthCode)
}

// HandleOIDCCallback is the handler function for any callback in OAuth. It will
// eventually redirect the user to the clientRedirect in the oauthEnv
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (o *oauthEnv) HandleOIDCCallback(c *gin.Context) {
	session, err := o.sessionStore.Get(c.Request, oktaCookieName)
	if err != nil {
		c.AbortWithError(http.StatusForbidden, err) //nolint:errcheck

		return
	}

	// Check the state that was returned in the query string is the same as the above state
	if c.Query("state") == "" || c.Query("state") != session.Values["oauth_state"] {
		c.AbortWithError(http.StatusForbidden, ErrOIDCUnexpectedState) //nolint:errcheck

		return
	}

	// Make sure the code was provided
	if errStr := c.Query("error"); errStr != "" {
		err = Error(errStr)
		c.AbortWithError(http.StatusForbidden, //nolint:errcheck
			fmt.Errorf("authorization server returned an error: %w", err))

		return
	}

	// Make sure the code was provided
	if c.Query("code") == "" {
		c.AbortWithError(http.StatusForbidden, ErrOIDCUnavailableCode) //nolint:errcheck

		return
	}

	token, err := o.Exchange(
		context.Background(),
		c.Query("code"),
		oauth2.SetAuthURLParam("code_verifier", session.Values["oauth_code_verifier"].(string)),
	)
	if err != nil {
		c.AbortWithError(http.StatusUnauthorized, err) //nolint:errcheck

		return
	}

	// Extract the ID Token from OAuth2 token.
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		c.AbortWithError(http.StatusUnauthorized, ErrOIDCMissingToken) //nolint:errcheck

		return
	}

	_, err = o.params.verifyToken(rawIDToken)
	if err != nil {
		c.AbortWithError(http.StatusForbidden, err) //nolint:errcheck

		return
	}

	session.Values["access_token"] = token.AccessToken

	if err = session.Save(c.Request, c.Writer); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	c.Redirect(http.StatusFound, o.clientRedirect)
}

// HandleOIDCLogin handles redirecting the user to the Okta login, as well as
// providing it challenge codes.
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (o *oauthEnv) HandleOIDCLogin(c *gin.Context) { //nolint:funlen
	c.Header("Cache-Control", "no-cache") // See https://github.com/okta/samples-golang/issues/20

	session, err := o.sessionStore.Get(c.Request, oktaCookieName)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}
	// Generate a random state parameter for CSRF security
	oauthState := randstr.Hex(16) //nolint:gomnd

	// Create the PKCE code verifier and code challenge
	oauthCodeVerifier, err := oauthUtils.GenerateCodeVerifierWithLength(50) // nolint:gomnd
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}
	// get sha256 hash of the code verifier
	oauthCodeChallenge := oauthCodeVerifier.CodeChallengeS256()

	session.Values["oauth_state"] = oauthState
	session.Values["oauth_code_verifier"] = oauthCodeVerifier.String()

	if err = session.Save(c.Request, c.Writer); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	redirectURI := o.AuthCodeURL(
		oauthState,
		oauth2.SetAuthURLParam("code_challenge", oauthCodeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)

	c.Redirect(http.StatusFound, redirectURI)
}

// verifyToken passes the token and the oAuthParameters through a JWT verifier
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (p oAuthParameters) verifyToken(t string) (*verifier.Jwt, error) {
	tv := map[string]string{}
	tv["aud"] = p.clientID
	jv := verifier.JwtVerifier{
		Issuer:           p.issuer,
		ClaimsToValidate: tv,
	}

	result, err := jv.New().VerifyIdToken(t)
	if err != nil {
		return nil, err
	}

	if result != nil {
		return result, nil
	}

	return nil, ErrCouldNotVerifyToken
}

func doRequestExpectingJSONWithStringValues(r *http.Request) (map[string]string, error) {
	m := make(map[string]string)

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return m, err
	}

	err = json.Unmarshal(body, &m)

	return m, err
}

// getProfileData takes a HTTP request (containing things like the cookie) and
// will get user information in a map from Okta
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (s *Server) getProfileData(r *http.Request) (map[string]string, error) {
	m := make(map[string]string)

	session, err := s.webOAuth.sessionStore.Get(r, oktaCookieName)
	if err != nil || session.Values["access_token"] == nil || session.Values["access_token"] == "" {
		return m, nil // nolint:nilerr
	}

	ctx, cnlFunc := context.WithTimeout(context.Background(), time.Minute)
	defer cnlFunc()

	req, err := http.NewRequestWithContext(ctx, "GET", s.webOAuth.params.issuer+"/v1/userinfo", nil)
	if err != nil {
		return m, err
	}

	token, ok := session.Values["access_token"].(string)
	if !ok {
		return m, nil
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Accept", "application/json")

	m, err = doRequestExpectingJSONWithStringValues(req)

	return m, err
}
