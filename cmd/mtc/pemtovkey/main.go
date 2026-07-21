// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// pemtovkey is a utility to convert a PEM containing an ML-DSA-44 key and MTC CA subject to a vkey representation.
package main

import (
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"

	fnote "github.com/transparency-dev/formats/note"
	"golang.org/x/crypto/cryptobyte"
	cryptobyte_asn1 "golang.org/x/crypto/cryptobyte/asn1"
)

const (
	algMLDSA44 = 0x06
)

var (
	certFile = flag.String("cert", "", "Path to x509 PEM file to read")

	mtcTrustAnchorExperimental = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 44363, 47, 1}
	oidPublicKeyMLDSA44        = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 3, 17}
)

func main() {
	flag.Parse()

	pubKBytes, name := keyAndIDFromCert(*certFile)

	h := keyHashMLDSA(name, pubKBytes)
	vkey := fmt.Sprintf("%s+%08x+%s", name, h, base64.StdEncoding.EncodeToString(pubKBytes))

	if _, err := fnote.NewVerifierForCosignatureV1(vkey); err != nil {
		exit("Generated vkey is invalid: %v", err)
	}

	fmt.Println(vkey)
}

func keyAndIDFromCert(path string) ([]byte, string) {
	c, err := os.ReadFile(path)
	if err != nil {
		exit("Failed to read file %q: %v", path, err)
	}

	der, _ := pem.Decode(c)
	if der == nil {
		exit("Invalid DER")
	}
	crt, err := x509.ParseCertificate(der.Bytes)
	if err != nil {
		exit("Failed to parse certificate: %v", err)
	}

	var relOID string
	for _, n := range crt.Subject.Names {
		if mtcTrustAnchorExperimental.Equal(n.Type) {
			v, ok := n.Value.(string)
			if !ok {
				exit("Found MTC trust anchor ID %q, but couldn't convert value to a string", n)
			}
			relOID = v
			break
		}
	}
	if relOID == "" {
		exit("No MTC trust anchor ID present in cert.")
	}
	name := fmt.Sprintf("oid/1.3.6.1.4.1.44363.47.1.%s", relOID)

	pub, err := parseSPKI(crt.RawSubjectPublicKeyInfo)
	if err != nil {
		exit("Failed to parse SPKI: %v", err)
	}
	if l := len(pub); l != 1312 {
		exit("Invalid public key length %d, expected 1312", l)
	}

	return append([]byte{algMLDSA44}, pub...), name
}

func parseSPKI(b []byte) ([]byte, error) {
	spki := cryptobyte.String(b)
	if !spki.ReadASN1(&spki, cryptobyte_asn1.SEQUENCE) {
		return nil, errors.New("x509: malformed spki")
	}
	var pkAISeq cryptobyte.String
	if !spki.ReadASN1(&pkAISeq, cryptobyte_asn1.SEQUENCE) {
		return nil, errors.New("x509: malformed public key algorithm identifier")
	}
	pkAI, err := parseAI(pkAISeq)
	if err != nil {
		return nil, err
	}
	if !pkAI.Algorithm.Equal(oidPublicKeyMLDSA44) {
		return nil, errors.New("public key type not ML-DSA-44")
	}
	var spk asn1.BitString
	if !spki.ReadASN1BitString(&spk) {
		return nil, errors.New("x509: malformed subjectPublicKey")
	}
	return spk.Bytes, nil
}

func parseAI(der cryptobyte.String) (pkix.AlgorithmIdentifier, error) {
	ai := pkix.AlgorithmIdentifier{}
	if !der.ReadASN1ObjectIdentifier(&ai.Algorithm) {
		return ai, errors.New("x509: malformed OID")
	}
	if der.Empty() {
		return ai, nil
	}
	var params cryptobyte.String
	var tag cryptobyte_asn1.Tag
	if !der.ReadAnyASN1Element(&params, &tag) {
		return ai, errors.New("x509: malformed parameters")
	}
	ai.Parameters.Tag = int(tag)
	ai.Parameters.FullBytes = params
	return ai, nil
}

func keyHashMLDSA(name string, key []byte) uint32 {
	h := sha256.New()
	h.Write([]byte(name))
	h.Write([]byte("\n"))
	h.Write(key)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint32(sum)
}

func exit(m string, a ...any) {
	fmt.Printf(m+"\n", a...)
	os.Exit(1)
}
