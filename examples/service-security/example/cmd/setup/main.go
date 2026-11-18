package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

func token() (string, error) {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}
func prepareEnv() error {
	if _, err := os.Stat("local.env"); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	data, err := os.ReadFile("local.env.example")
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		text := strings.TrimPrefix(line, "export ")
		key, _, ok := strings.Cut(text, "=")
		if ok && (key == "JWT_SECRET" || strings.HasSuffix(key, "_SERVICE_TOKEN")) {
			value, err := token()
			if err != nil {
				return err
			}
			lines[i] = "export " + key + "='" + value + "'"
		}
	}
	return os.WriteFile("local.env", []byte(strings.Join(lines, "\n")), 0600)
}
func main() {
	if err := prepareEnv(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := prepareCertificates(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("local example configuration ready")
}
