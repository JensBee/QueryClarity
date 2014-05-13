/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.unihildesheim.iw.util;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.InternMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Utilities for working with byte arrays.
 *
 * @author Jens Bertram
 */
public final class ByteArrayUtils {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ByteArrayUtils.class);

  /**
   * Internal {@link Map} to cache string representations of UTF8 bytes.
   */
  private static final transient Map<byte[], String> INTERN8
      = Collections.synchronizedMap(new InternMap<byte[], String>(100000));
  /**
   * Internal {@link Map} to cache string representations of UTF16 bytes.
   */
  private static final transient Map<byte[], String> INTERN16
      = Collections.synchronizedMap(new InternMap<byte[], String>(100000));

  /**
   * Private empty constructor for utility class.
   */
  private ByteArrayUtils() {
    // empty
  }

  /**
   * Convert the bytes encoded as UTF-8 to string. This does not check if the
   * bytes are valid. If the array contains non UTF-8 bytes the behavior is
   * undefined.
   *
   * @param bytes UTF-8 bytes array
   * @return String created from bytes
   */
  public static String utf8ToString(final byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes were null.");
    }
    String str = INTERN8.get(bytes);
    if (str == null) {
      try {
        str = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException ex) {
        LOG.error("Error encoding bytes.", ex);
      }
      INTERN8.put(Arrays.copyOf(bytes, bytes.length), str);
    }
    return str;
  }

  /**
   * Convert the bytes contained in the {@link ByteArray} encoded as UTF-8 to
   * string. This does not check if the bytes are valid. If the array contains
   * non UTF-8 bytes the behavior is undefined.
   *
   * @param bytes {@link ByteArray} containing an UTF-8 bytes array
   * @return String created from bytes
   */
  public static String utf8ToString(final ByteArray bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes were null.");
    }
    return utf8ToString(bytes.bytes);
  }

  /**
   * Convert the bytes encoded as UTF-16 to string. This does not check if the
   * bytes are valid. If the array contains non UTF-16 bytes the behavior is
   * undefined.
   *
   * @param bytes UTF-8 bytes array
   * @return String created from bytes
   */
  public static String utf16ToString(final byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes were null.");
    }
    String str = INTERN16.get(bytes);
    if (str == null) {
      try {
        str = new String(bytes, "UTF-16");
      } catch (UnsupportedEncodingException ex) {
        LOG.error("Error encoding bytes.", ex);
      }
      INTERN16.put(Arrays.copyOf(bytes, bytes.length), str);
    }
    return str;
  }

  /**
   * Convert the bytes contained in the {@link ByteArray} encoded as UTF-16 to
   * string. This does not check if the bytes are valid. If the array contains
   * non UTF-16 bytes the behavior is undefined.
   *
   * @param bytes {@link ByteArray} containing an UTF-16 bytes array
   * @return String created from bytes
   */
  public static String utf16ToString(final ByteArray bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes were null.");
    }
    return utf8ToString(bytes.bytes);
  }
}
