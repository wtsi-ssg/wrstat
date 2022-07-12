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

	"github.com/gin-gonic/gin"
	"github.com/gorilla/sessions"
	verifier "github.com/okta/okta-jwt-verifier-golang"
	oauthUtils "github.com/okta/okta-jwt-verifier-golang/utils"
	"github.com/thanhpk/randstr"

	"golang.org/x/oauth2"
)

const oktaCookieName = "okta-hosted-login-session-store"

var sessionStore = sessions.NewCookieStore([]byte(oktaCookieName))

type oAuthParameters struct {
	issuer       string
	clientID     string
	clientSecret string
}

// newOktaOauthConfig returns a oauth2.Config with the appropriate values based
// on defaults, the paramenters in oAuthParameters and the callack URL
func (p oAuthParameters) newOktaOauthConfig(callback string) oauth2.Config {
	return oauth2.Config{
		RedirectURL:  callback,
		ClientID:     p.clientID,
		ClientSecret: p.clientSecret,
		Scopes:       []string{"openid", "profile", "email"},
		Endpoint: oauth2.Endpoint{
			AuthURL:   p.issuer + "/v1/authorize",
			TokenURL:  p.issuer + "/v1/token",
			AuthStyle: oauth2.AuthStyleInParams,
		},
	}
}

type oauthEnv struct {
	oauth2.Config
	params         oAuthParameters
	clientRedirect string
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

	s.webOAuth = &oauthEnv{
		Config:         params.newOktaOauthConfig(s.Address + EndpointAuthCallback),
		params:         params,
		clientRedirect: "/",
	}

	s.cliOAuth = &oauthEnv{
		Config:         params.newOktaOauthConfig(s.Address + EndpointAuthCLICallback),
		params:         params,
		clientRedirect: EndpointCLIAuthCode,
	}

	s.router.GET(EndpointAuthCallback, s.webOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCLogin, s.webOAuth.HandleOIDCLogin)

	s.router.GET(EndpointAuthCLICallback, s.cliOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCCLILogin, s.cliOAuth.HandleOIDCLogin)

	s.router.GET(EndpointCLIAuthCode, func(ctx *gin.Context) {
		cookie, err := ctx.Request.Cookie(oktaCookieName)
		if err != nil {
			ctx.Writer.Write([]byte(err.Error()))
			return
		}
		ctx.Writer.Write([]byte(cookie.Value))
	})
}

// HandleOIDCCallback is the handler function for any callback in OAuth. It will
// eventually redirect the user to the clientRedirect in the oauthEnv
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (o *oauthEnv) HandleOIDCCallback(c *gin.Context) {
	session, err := sessionStore.Get(c.Request, oktaCookieName)
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
	_, err = o.params.verifyToken(rawIDToken)

	if err != nil {
		c.AbortWithError(http.StatusForbidden, err)
		return
	} else {
		session.Values["access_token"] = token.AccessToken

		session.Save(c.Request, c.Writer)
	}

	c.Redirect(http.StatusFound, o.clientRedirect)
}

// HandleOIDCLogin handles redirecting the user to the Okta login, as well as
// providing it challenge codes.
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (o *oauthEnv) HandleOIDCLogin(c *gin.Context) {
	c.Header("Cache-Control", "no-cache") // See https://github.com/okta/samples-golang/issues/20

	session, err := sessionStore.Get(c.Request, oktaCookieName)
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

	return nil, fmt.Errorf("token could not be verified")
}

// getProfileData takes a HTTP request (containing things like the cookie) and
// will get user information in a map from Okta
// See: https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
func (s *Server) getProfileData(r *http.Request) (map[string]string, error) {
	m := make(map[string]string)

	session, err := sessionStore.Get(r, oktaCookieName)

	if err != nil || session.Values["access_token"] == nil || session.Values["access_token"] == "" {
		return m, nil
	}

	reqUrl := s.webOAuth.params.issuer + "/v1/userinfo"

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
