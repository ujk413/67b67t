package ru.b67t.roomkey

import org.junit.Assert.assertEquals
import org.junit.Test

class RoomKeyDerivationTest {
  @Test fun matchesExistingRoomV2Parameters() {
    val key = RoomKeyDerivation.derive("cGFzc3BocmFzZQ==", "room-v2:fixture:secret".toByteArray(Charsets.UTF_8))
    assertEquals("8ed0b8805f423937b720f1da87d0c8db70f33bbafaca62e15351317dcaaded3c", key.hex())
  }

  @Test fun matchesUtf8PasswordAndSalt() {
    val key = RoomKeyDerivation.derive("пароль", "соль".toByteArray(Charsets.UTF_8))
    assertEquals("01512586e3bfe6a1f6cf55c8907a55db1fe4a48ef88201069bdaa320a9addc87", key.hex())
  }

  private fun ByteArray.hex() = joinToString("") { "%02x".format(it.toInt() and 255) }
}
