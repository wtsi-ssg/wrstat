/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Rosie Kern <rk18@sanger.ac.uk>
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

package encode

import (
	"encoding/base64"
	"unsafe"
)

var encoding = base64.NewEncoding("+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") //nolint:gochecknoglobals,lll

// Base64Encode encodes the given string to a lexically ordered base64.
func Base64Encode(val string) string {
	if val == "" {
		return ""
	}

	buf := make([]byte, encoding.EncodedLen(len(val)))

	encoding.Encode(buf, unsafe.Slice(unsafe.StringData(val), len(val)))

	return unsafe.String(&buf[0], len(buf))
}

// Base64Decode decodes the given encoded string from a lexically ordered Base64
// encoding.
func Base64Decode(val string) (string, error) {
	if val == "" {
		return "", nil
	}

	str, err := encoding.DecodeString(val)
	if err != nil {
		return "", err
	}

	return unsafe.String(&str[0], len(str)), nil
}
