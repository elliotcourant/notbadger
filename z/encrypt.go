package z

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
)

// GenerateIV generates IV.
func GenerateIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	_, err := rand.Read(iv)
	return iv, err
}

// XORBlock encrypts the given data with AES and XOR's with IV.
// Can be used for both encryption and decryption. IV is of
// AES block size.
func XORBlock(src, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)
	dst := make([]byte, len(src))
	stream.XORKeyStream(dst, src)
	return dst, nil
}
