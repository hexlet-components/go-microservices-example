package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"time"
)

func serial() (*big.Int, error) {
	value, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err == nil {
		value.Add(value, big.NewInt(1))
	}
	return value, err
}
func prepareCertificates() error {
	if _, err := os.Stat("certs/localhost-test.key"); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll("certs", 0700); err != nil {
		return err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caSerial, err := serial()
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: caSerial, Subject: pkix.Name{CommonName: "Local course CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serverSerial, err := serial()
	if err != nil {
		return err
	}
	server := &x509.Certificate{SerialNumber: serverSerial, Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(7 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return err
	}
	for path, block := range map[string]*pem.Block{"certs/ca.crt": {Type: "CERTIFICATE", Bytes: caDER}, "certs/localhost.crt": {Type: "CERTIFICATE", Bytes: serverDER}, "certs/localhost-test.key": {Type: "PRIVATE KEY", Bytes: keyDER}} {
		mode := os.FileMode(0644)
		if path == "certs/localhost-test.key" {
			mode = 0600
		}
		if err = os.WriteFile(path, pem.EncodeToMemory(block), mode); err != nil {
			return err
		}
	}
	return nil
}
