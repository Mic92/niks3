package client

// nixbase32 implements Nix's base32 encoding scheme.
// This is a custom base32 encoding that omits certain characters (e, o, u, t)
// to avoid confusion and uses a specific alphabet.

const nixBase32Alphabet = "0123456789abcdfghijklmnpqrsvwxyz"

// EncodeNixBase32 encodes bytes into Nix's base32 format.
// This implementation is based on Nix's BaseNix32::encode in src/libutil/base-nix-32.cc
func EncodeNixBase32(input []byte) string {
	if len(input) == 0 {
		return ""
	}

	// Calculate the encoded length: (len * 8 - 1) / 5 + 1
	length := (len(input)*8-1)/5 + 1

	result := make([]byte, 0, length)

	// The C++ implementation builds the string by iterating n from high to low
	// and appending characters. This means the character for n=length-1 comes first.
	for n := length - 1; n >= 0; n-- {
		b := n * 5
		i := b / 8
		j := b % 8

		// Extract 5 bits starting at position (i, j)
		var c byte
		if i < len(input) {
			c = input[i] >> j
		}
		if i+1 < len(input) {
			c |= input[i+1] << (8 - j)
		}

		// Take only the lower 5 bits and map to alphabet
		result = append(result, nixBase32Alphabet[c&0x1f])
	}

	return string(result)
}
