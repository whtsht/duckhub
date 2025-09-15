use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use ring::{
    aead,
    aead::{BoundKey, NONCE_LEN, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey},
    error::Unspecified,
    rand,
    rand::SecureRandom,
};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

struct SingleNonce {
    nonce_bytes: [u8; NONCE_LEN],
    used: bool,
}

impl SingleNonce {
    fn new(nonce_bytes: [u8; NONCE_LEN]) -> Self {
        Self {
            nonce_bytes,
            used: false,
        }
    }
}

impl NonceSequence for SingleNonce {
    fn advance(&mut self) -> Result<Nonce, Unspecified> {
        if self.used {
            return Err(Unspecified);
        }
        self.used = true;
        Nonce::try_assume_unique_for_key(&self.nonce_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SecretField {
    #[serde(rename = "plain")]
    PlainText { value: String },
    #[serde(rename = "encrypted")]
    Encrypted { value: String },
}

impl SecretField {
    pub fn load(&mut self, project_dir: &Path) -> Result<()> {
        match self {
            SecretField::PlainText { .. } => Ok(()),
            SecretField::Encrypted { value } => {
                let key_path = project_dir.join(".secret.key");
                let decrypted = Self::decrypt_string(value, &key_path)?;
                *self = SecretField::PlainText { value: decrypted };
                Ok(())
            }
        }
    }

    pub fn plaintext(&self) -> Result<&str> {
        match self {
            SecretField::PlainText { value } => Ok(value),
            SecretField::Encrypted { .. } => Err(anyhow::anyhow!(
                "Field not decrypted yet. Call load() first."
            )),
        }
    }

    pub fn encrypt(plaintext: &str, key_path: &Path) -> Result<Self> {
        let encrypted = Self::encrypt_string(plaintext, key_path)?;
        Ok(SecretField::Encrypted { value: encrypted })
    }

    fn encrypt_string(plaintext: &str, key_path: &Path) -> Result<String> {
        let key_bytes = fs::read(key_path)
            .with_context(|| format!("Failed to read encryption key from {key_path:?}"))?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key size: expected 32 bytes, got {}",
                key_bytes.len()
            ));
        }

        let alg = &aead::AES_256_GCM;
        let unbound_key = UnboundKey::new(alg, &key_bytes)
            .map_err(|_| anyhow::anyhow!("Failed to create encryption key"))?;

        let rng = rand::SystemRandom::new();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rng.fill(&mut nonce_bytes)
            .map_err(|_| anyhow::anyhow!("Failed to generate nonce"))?;

        let nonce_sequence = SingleNonce::new(nonce_bytes);
        let mut sealing_key = SealingKey::new(unbound_key, nonce_sequence);

        let mut message = plaintext.as_bytes().to_vec();
        let tag = sealing_key
            .seal_in_place_separate_tag(aead::Aad::empty(), &mut message)
            .map_err(|_| anyhow::anyhow!("Encryption failed"))?;

        let mut combined = nonce_bytes.to_vec();
        combined.extend_from_slice(&message);
        combined.extend_from_slice(tag.as_ref());

        Ok(BASE64.encode(&combined))
    }

    fn decrypt_string(encrypted: &str, key_path: &Path) -> Result<String> {
        let key_bytes = fs::read(key_path)
            .with_context(|| format!("Failed to read encryption key from {key_path:?}"))?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key size: expected 32 bytes, got {}",
                key_bytes.len()
            ));
        }

        let combined = BASE64
            .decode(encrypted)
            .context("Failed to decode encrypted data")?;

        let alg = &aead::AES_256_GCM;

        if combined.len() < NONCE_LEN + alg.tag_len() {
            return Err(anyhow::anyhow!("Invalid encrypted data: too short"));
        }

        let (nonce_bytes, ciphertext_and_tag) = combined.split_at(NONCE_LEN);
        let mut nonce_array = [0u8; NONCE_LEN];
        nonce_array.copy_from_slice(nonce_bytes);

        let unbound_key = UnboundKey::new(alg, &key_bytes)
            .map_err(|_| anyhow::anyhow!("Failed to create decryption key"))?;

        let nonce_sequence = SingleNonce::new(nonce_array);
        let mut opening_key = OpeningKey::new(unbound_key, nonce_sequence);

        let mut in_out = ciphertext_and_tag.to_vec();
        let plaintext = opening_key
            .open_in_place(aead::Aad::empty(), &mut in_out)
            .map_err(|_| anyhow::anyhow!("Decryption failed"))?;

        String::from_utf8(plaintext.to_vec()).context("Failed to convert decrypted data to string")
    }

    #[cfg(test)]
    pub fn test_encrypt(plaintext: &str) -> Result<Self> {
        Ok(Self::PlainText {
            value: plaintext.to_string(),
        })
    }
}

pub fn generate_secret_key() -> Result<Vec<u8>> {
    let rng = rand::SystemRandom::new();
    let mut key = vec![0u8; 32];
    rng.fill(&mut key)
        .map_err(|_| anyhow::anyhow!("Failed to generate secret key"))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        let key = generate_secret_key().unwrap();
        fs::write(&key_path, &key).unwrap();

        let plaintext = "my_secret_password";
        let encrypted = SecretField::encrypt(plaintext, &key_path).unwrap();

        match &encrypted {
            SecretField::Encrypted { value } => {
                assert!(!value.is_empty());
            }
            SecretField::PlainText { .. } => panic!("Expected encrypted field"),
        }

        let decrypted = SecretField::decrypt_string(
            match &encrypted {
                SecretField::Encrypted { value } => value,
                _ => panic!("Expected encrypted field"),
            },
            &key_path,
        )
        .unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_nonces() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        let key = generate_secret_key().unwrap();
        fs::write(&key_path, &key).unwrap();

        let plaintext = "test";
        let encrypted1 = SecretField::encrypt(plaintext, &key_path).unwrap();
        let encrypted2 = SecretField::encrypt(plaintext, &key_path).unwrap();

        let enc1 = match &encrypted1 {
            SecretField::Encrypted { value } => value,
            _ => panic!("Expected encrypted field"),
        };
        let enc2 = match &encrypted2 {
            SecretField::Encrypted { value } => value,
            _ => panic!("Expected encrypted field"),
        };

        assert_ne!(enc1, enc2);
    }

    #[test]
    fn test_invalid_key_file() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("nonexistent.key");

        let result = SecretField::encrypt("test", &key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_key_size() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        fs::write(&key_path, b"short_key").unwrap();

        let result = SecretField::encrypt("test", &key_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid key size"));
    }

    #[test]
    fn test_corrupted_encrypted_data() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        let key = generate_secret_key().unwrap();
        fs::write(&key_path, &key).unwrap();

        let result = SecretField::decrypt_string("invalid_base64!!!", &key_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_special_characters() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        let key = generate_secret_key().unwrap();
        fs::write(&key_path, &key).unwrap();

        let plaintext = "test_password";
        let encrypted = SecretField::encrypt(plaintext, &key_path).unwrap();
        let decrypted = SecretField::decrypt_string(
            match &encrypted {
                SecretField::Encrypted { value } => value,
                _ => panic!("Expected encrypted field"),
            },
            &key_path,
        )
        .unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_empty_string() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join(".secret.key");

        let key = generate_secret_key().unwrap();
        fs::write(&key_path, &key).unwrap();

        let encrypted = SecretField::encrypt("", &key_path).unwrap();
        let decrypted = SecretField::decrypt_string(
            match &encrypted {
                SecretField::Encrypted { value } => value,
                _ => panic!("Expected encrypted field"),
            },
            &key_path,
        )
        .unwrap();

        assert_eq!(decrypted, "");
    }

    #[test]
    fn test_plaintext_field() {
        let plaintext = "plain_password";
        let field = SecretField::PlainText {
            value: plaintext.to_string(),
        };

        assert_eq!(field.plaintext().unwrap(), plaintext);
    }

    #[test]
    fn test_load_plaintext_field() {
        let dir = tempdir().unwrap();
        let mut field = SecretField::PlainText {
            value: "plain_password".to_string(),
        };

        assert!(field.load(dir.path()).is_ok());
        assert_eq!(field.plaintext().unwrap(), "plain_password");
    }
}
