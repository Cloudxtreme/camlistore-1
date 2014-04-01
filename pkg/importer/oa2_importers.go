/*
Copyright 2014 The Camlistore Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package importer imports content from third-party websites.
package importer

import (
	"fmt"
	"log"
	"net/http"
	"net/url"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/httputil"

	"camlistore.org/third_party/code.google.com/p/goauth2/oauth"
)

const (
	AcctAttrUserID      = "userID"
	AcctAttrUserFirst   = "givenName"
	AcctAttrUserLast    = "familyName"
	AcctAttrAccessToken = "oauthAccessToken"
)

// ExtendedOAuth2 provides implementation for some common importer methods regarding authentication.
//
// The oauthConfig is used in the authentications - think Scope and AuthURL.
//
// The getUserInfo function (if provided) should return the
// user ID, first name and last name of the user.
type ExtendedOAuth2 struct {
	OAuth2
	oauthConfig oauth.Config
	getUserInfo func(ctx *context.Context, accessToken string) (string, string, string, error)
}

// NewExtendedOAuth2 returns a default implementation of
// some common methods for OAuth2-based importers.
func NewExtendedOAuth2(oauthConfig oauth.Config,
	getUserInfo func(ctx *context.Context, accessToken string) (string, string, string, error),
) ExtendedOAuth2 {
	return ExtendedOAuth2{oauthConfig: oauthConfig, getUserInfo: getUserInfo}
}

// NeedsAPIKey returns whether the importer needs an API key - returns constant true.
func (im ExtendedOAuth2) NeedsAPIKey() bool { return true }

// IsAccountReady returns whether the account has been properly configured
// - whether the user ID and access token has been stored in the given account node.
func (im ExtendedOAuth2) IsAccountReady(acctNode *Object) (ok bool, err error) {
	if acctNode.Attr(AcctAttrUserID) != "" &&
		acctNode.Attr(AcctAttrAccessToken) != "" {
		return true, nil
	}
	return false, nil
}

// SummarizeAccount returns a summary for the account if it is configured,
// or an error string otherwise.
func (im ExtendedOAuth2) SummarizeAccount(acct *Object) string {
	ok, err := im.IsAccountReady(acct)
	if err != nil {
		return "Not configured; error = " + err.Error()
	}
	if !ok {
		return "Not configured"
	}
	if acct.Attr(AcctAttrUserFirst) == "" &&
		acct.Attr(AcctAttrUserLast) == "" {
		return fmt.Sprintf("userid %s", acct.Attr(AcctAttrUserID))
	}
	return fmt.Sprintf("userid %s (%s %s)",
		acct.Attr(AcctAttrUserID),
		acct.Attr(AcctAttrUserFirst),
		acct.Attr(AcctAttrUserLast))
}

func (im ExtendedOAuth2) ServeSetup(w http.ResponseWriter, r *http.Request, ctx *SetupContext) error {
	oauthConfig, err := im.auth(ctx)
	if err == nil {
		// we will get back this with the token, so use it for preserving account info
		state := "acct:" + ctx.AccountNode.PermanodeRef().String()
		http.Redirect(w, r, oauthConfig.AuthCodeURL(state), 302)
	}
	return err
}

// CallbackURLParameters returns the needed callback parameters - empty for Google Picasa.
func (im ExtendedOAuth2) CallbackURLParameters(acctRef blob.Ref) url.Values {
	return url.Values{}
}

func (im ExtendedOAuth2) ServeCallback(w http.ResponseWriter, r *http.Request, ctx *SetupContext) {
	if im.getUserInfo == nil {
		panic("No getUserInfo is provided, don't use the default ServeCallback!")
	}
	if ctx.Context == nil {
		// FIXME(tgulacsi): context.TODO() returns a nil context, but we need a
		// proper (non-nil) context to be able to set its HTTP Client.
		log.Fatalf("ERROR! empty Context in %#v.ServeCallback(ctx=%#v)", im, ctx)
	}

	oauthConfig, err := im.auth(ctx)
	if err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error getting oauth config: %v", err))
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Expected a GET", 400)
		return
	}
	code := r.FormValue("code")
	if code == "" {
		http.Error(w, "Expected a code", 400)
		return
	}
	transport := &oauth.Transport{Config: oauthConfig}
	token, err := transport.Exchange(code)
	log.Printf("Token = %#v, error %v", token, err)
	if err != nil {
		log.Printf("Token Exchange error: %v", err)
		httputil.ServeError(w, r, fmt.Errorf("token exchange error: %v", err))
		return
	}

	// copy the client before modifying it
	client := *ctx.Context.HTTPClient()
	client.Transport = transport
	ctx.Context.SetHTTPClient(&client)

	uId, uFirstName, uLastName, err := im.getUserInfo(ctx.Context, token.AccessToken)
	if err != nil {
		log.Printf("Couldn't get username: %v", err)
		httputil.ServeError(w, r, fmt.Errorf("can't get username: %v", err))
		return
	}

	if err := ctx.AccountNode.SetAttrs(
		AcctAttrUserID, uId,
		AcctAttrUserFirst, uFirstName,
		AcctAttrUserLast, uLastName,
		AcctAttrAccessToken, token.AccessToken,
	); err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error setting attribute: %v", err))
		return
	}
	http.Redirect(w, r, ctx.AccountURL(), http.StatusFound)
}

func (im ExtendedOAuth2) auth(ctx *SetupContext) (*oauth.Config, error) {
	clientId, secret, err := ctx.Credentials()
	if err != nil {
		return nil, err
	}
	conf := im.oauthConfig
	conf.ClientId, conf.ClientSecret, conf.RedirectURL = clientId, secret, ctx.CallbackURL()
	return &conf, nil
}
