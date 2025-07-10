// Nix uses a custom base32 alphabet
const NIX_BASE32_ALPHABET: &[u8] = b"0123456789abcdfghijklmnpqrsvwxyz";

pub fn encode(input: &[u8]) -> String {
    if input.is_empty() {
        return String::new();
    }

    // Calculate the output length
    let len = (input.len() * 8 - 1) / 5 + 1;

    let mut result = String::with_capacity(len);

    // Process from the highest bit position down to 0
    for n in (0..len).rev() {
        let b = n * 5;
        let i = b / 8;
        let j = b % 8;

        // Extract 5 bits starting at bit position b
        let mut c = if i < input.len() { input[i] >> j } else { 0 };

        if i + 1 < input.len() && j > 3 {
            c |= input[i + 1] << (8 - j);
        }

        result.push(NIX_BASE32_ALPHABET[(c & 0x1f) as usize] as char);
    }

    result
}

pub fn hash_to_nix_string(algo: &str, hash: &[u8]) -> String {
    format!("{}:{}", algo, encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn test_nix_base32_encode() {
        // Test vectors from Nix
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"\x00"), "00");
        assert_eq!(encode(b"\xff"), "7z");

        // Test a known hash
        let mut hasher = Sha256::new();
        hasher.update(b"hello");
        let hash = hasher.finalize();
        let encoded = encode(&hash);

        // This is the expected nix32 encoding of SHA256("hello")
        // Verified with: echo -n "hello" | nix hash file --type sha256 --base32 /dev/stdin
        assert_eq!(
            encoded,
            "094qif9n4cq4fdg459qzbhg1c6wywawwaaivx0k0x8xhbyx4vwic"
        );
    }

    #[test]
    fn test_hash_to_nix_string() {
        let hash = [0x12, 0x34, 0x56, 0x78];
        let result = hash_to_nix_string("sha256", &hash);
        assert!(result.starts_with("sha256:"));
    }
}
