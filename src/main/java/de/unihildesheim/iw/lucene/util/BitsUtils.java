/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class BitsUtils {

  /**
   * Convert plain {@link Bits} instance to a {@link FixedBitSet} instance.
   * @param bits Bits to convert
   * @return New instance or {@code null} if {@code bits} were {@code null}.
   */
  @Contract("null -> null")
  @Nullable
  public static FixedBitSet Bits2FixedBitSet(@Nullable final Bits bits) {
    if (bits == null) {
      return null;
    }
    final int bitCount = bits.length();
    final FixedBitSet fbs = new FixedBitSet(bitCount);
    for (int i =0; i< bitCount; i++) {
      if (bits.get(i)) {
        fbs.set(i);
      }
    }
    return fbs;
  }
}
