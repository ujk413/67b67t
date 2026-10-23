package ru.b67t.expressive;

import java.net.URI;
import java.util.Locale;

/** Incoming intents are untrusted; never read app-private paths on their behalf. */
public final class IncomingSharePolicy {
  private IncomingSharePolicy() {}
  public static boolean allows(String value, String ownPackage) {
    try {
      URI uri = new URI(value);
      String authority = uri.getRawAuthority();
      if (!"content".equalsIgnoreCase(uri.getScheme()) || authority == null ||
          !authority.matches("[A-Za-z0-9._-]{1,250}") || uri.getRawPath() == null ||
          uri.getRawPath().length() > 4096) return false;
      String source = authority.toLowerCase(Locale.ROOT), own = ownPackage.toLowerCase(Locale.ROOT);
      return !source.equals(own) && !source.startsWith(own + ".");
    } catch (Exception ignored) { return false; }
  }
}
