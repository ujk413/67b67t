package ru.b67t.roomkey

import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

internal object RoomKeyDerivation {
  // Keep the existing room-v2 parameters: all previous ciphertext stays readable.
  fun derive(password: String, salt: ByteArray): ByteArray {
    val characters = password.toCharArray()
    val spec = PBEKeySpec(characters, salt, 250000, 256)
    try {
      return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).encoded
    } finally {
      spec.clearPassword()
      characters.fill('\u0000')
    }
  }
}
