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
package de.unihildesheim.iw.lucene.util;

import de.unihildesheim.iw.ByteArray;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.Objects;

/**
 * Utilities for working with {@link BytesRef}s.
 *
 * @author Jens Bertram
 */
public final class BytesRefUtils {

  /**
   * Private empty constructor for utility class.
   */
  private BytesRefUtils() {
    // empty
  }

  /**
   * Create a {@link ByteArray} by copying the bytes from the given {@link
   * BytesRef}.
   *
   * @param br {@link BytesRef} to copy from
   * @return New {@link ByteArray} instance with bytes copied from the {@link
   * BytesRef}
   */
  public static ByteArray toByteArray(final BytesRef br) {
    Objects.requireNonNull(br);
    return new ByteArray(br.bytes, br.offset, br.length);
  }

  /**
   * Compares the bytes contained in the {@link BytesRef} to those stored in the
   * {@link ByteArray}.
   *
   * @param br {@link BytesRef} to compare
   * @param ba {@link ByteArray} to compare
   * @return True, if both byte arrays are equal
   */
  public static boolean bytesEquals(final BytesRef br, final ByteArray ba) {
    Objects.requireNonNull(br);
    Objects.requireNonNull(ba);

    return ba.compareBytes(Arrays.copyOfRange(br.bytes, br.offset,
        br.length)) == 0;
  }
}
