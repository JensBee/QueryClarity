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

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class BitsUtils {
  /**
   * Convert plain {@link Bits} instance to a {@link BitSet} instance.
   *
   * @param bits Bits to convert
   * @return New instance or {@code null} if {@code bits} were {@code null}.
   */
  @Contract("null -> null; !null -> !null")
  @Nullable
  public static BitSet bits2BitSet(@Nullable final Bits bits) {
    if (bits == null) {
      return null;
    }
    if (BitSet.class.isInstance(bits)) {
      return (BitSet) bits;
    }

    final int bitCount = bits.length();
    final FixedBitSet fbs = new FixedBitSet(bitCount);
    StreamUtils.stream(bits).forEach(fbs::set);

    return fbs;
  }

  /**
   * Convert plain {@link Bits} instance to a {@link FixedBitSet} instance.
   *
   * @param bits Bits to convert
   * @return New instance or {@code null} if {@code bits} were {@code null}.
   */
  @Contract("null -> null; !null -> !null")
  @Nullable
  public static FixedBitSet bits2FixedBitSet(@Nullable final Bits bits) {
    if (bits == null) {
      return null;
    }
    final FixedBitSet fbs;
    if (FixedBitSet.class.isInstance(bits)) {
      fbs = ((FixedBitSet) bits).clone();
    } else if (BitSet.class.isInstance(bits)) {
      fbs = new FixedBitSet(bits.length());
      StreamUtils.stream((BitSet) bits).forEach(fbs::set);
    } else {
      final int bitCount = bits.length();
      fbs = new FixedBitSet(bitCount);
      StreamUtils.stream(bits).forEach(fbs::set);
    }
    return fbs;
  }

  /**
   * Convert an int array to a {@link FixedBitSet}.
   * @param intArr Ints
   * @return FixedBitSet with bits for the given ints set
   */
  public static FixedBitSet arrayToBits(final int... intArr) {
    if (intArr == null) {
      throw new IllegalArgumentException("Array was null.");
    }
    final FixedBitSet bits;
    if (intArr.length > 0) {
      final int[] sorted = new int[intArr.length];
      System.arraycopy(intArr, 0, sorted, 0, intArr.length);
      Arrays.sort(sorted);
      bits = new FixedBitSet(sorted[sorted.length - 1] + 1);
      Arrays.stream(sorted).forEach(bits::set);
    } else {
      bits = new FixedBitSet(0);
    }
    return bits;
  }
}
