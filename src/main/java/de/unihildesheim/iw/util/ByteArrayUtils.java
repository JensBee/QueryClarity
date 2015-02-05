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
import org.apache.lucene.util.UnicodeUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities for working with byte arrays.
 *
 * @author Jens Bertram
 */
public final class ByteArrayUtils {

  /**
   * Internal {@link Map} to cache string representations of UTF8 bytes.
   */
  private static final Map<byte[], String> INTERN8
      = Collections.synchronizedMap(new InternMap<>(100000));

  /**
   * Private empty constructor for utility class.
   */
  private ByteArrayUtils() {
    // empty
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
    return utf8ToString(Objects.requireNonNull(bytes,
        "Bytes were null.").bytes);
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
    String str = INTERN8.get(Objects.requireNonNull(bytes, "Bytes were null."));
    if (str == null) {
      final char[] ref = new char[bytes.length];
      final int len = UnicodeUtil.UTF8toUTF16(bytes, 0, bytes.length, ref);
      str = new String(ref, 0, len);
      INTERN8.put(Arrays.copyOf(bytes, bytes.length), str);
    }
    return str;
  }
}
