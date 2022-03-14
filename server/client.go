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

// package server provides a web server for a REST API and website.

package server

import (
	"net/http"

	"github.com/go-resty/resty/v2"
	"github.com/wtsi-ssg/wrstat/dgut"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrBadQuery = Error("bad query; check dir, group, user and type")

const ClientProtocol = "https://"

// GetWhereDataIs is a client call to a Server listening at the given
// domain:port url that queries where data is and returns the raw response body
// (JSON string), and that body converted in to dgut.DCSs.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// The other parameters correspond to arguments that dgut.Tree.Where() takes.
func GetWhereDataIs(url, cert, dir, groups, users, types, splits string) ([]byte, dgut.DCSs, error) {
	client := resty.New()

	if cert != "" {
		client.SetRootCertificate(cert)
	}

	resp, err := client.R().SetResult(dgut.DCSs{}).
		ForceContentType("application/json").
		SetQueryParams(map[string]string{
			"dir":    dir,
			"groups": groups,
			"users":  users,
			"types":  types,
			"splits": splits,
		}).
		Get(ClientProtocol + url + EndPointWhere)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, nil, ErrBadQuery
	}

	return resp.Body(), *resp.Result().(*dgut.DCSs), nil //nolint:forcetypeassert
}
