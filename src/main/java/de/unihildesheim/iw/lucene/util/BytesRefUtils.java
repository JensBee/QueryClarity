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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

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
   * Return a copy of the bytes.
   *
   * @param br {@link BytesRef} to copy bytes from
   * @return Copy of bytes from {@link BytesRef}
   */
  public static byte[] copyBytes(@NotNull final BytesRef br) {
    return Arrays.copyOfRange(br.bytes, br.offset, br.offset + br.length);
  }

  /**
   * Collector of {@link BytesRefHash}es for usage in stream expressions.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class MergingBytesRefHash {
    /**
     * Merged hash.
     */
    private final BytesRefHash hash;

    /**
     * Creates a new instance and initializes the local hash.
     */
    public MergingBytesRefHash() {
      this.hash = new BytesRefHash();
    }

    /**
     * Adds a {@link BytesRef} to the local hash.
     *
     * @param br BytesRef to add
     * @return the id the given bytes are hashed if there was no mapping for the
     * given bytes, otherwise <code>(-(id)-1)</code>. This guarantees that the
     * return value will always be &gt;= 0 if the given bytes haven't been
     * hashed before.
     */
    public int add(final BytesRef br) {
      return this.hash.add(br);
    }

    /**
     * Adds all {@link BytesRef} instances from the given {@link BytesRefHash}
     * to the local one.
     *
     * @param brh BytesRefHash to merge
     */
    @SuppressWarnings("ObjectAllocationInLoop")
    public void addAll(final BytesRefHash brh) {
      for (int i = brh.size() - 1; i >= 0; i--) {
        add(brh.get(i, new BytesRef()));
      }
    }

    /**
     * Adds all {@link BytesRef} instances from the given MergingBytesRefHash to
     * the local one.
     *
     * @param brh MergingBytesRefHash to merge
     */
    @SuppressWarnings("ObjectAllocationInLoop")
    public void addAll(final MergingBytesRefHash brh) {
      for (int i = brh.hash.size() - 1; i >= 0; i--) {
        add(brh.hash.get(i, new BytesRef()));
      }
    }

    /**
     * Stream the {@link BytesRef}s stored in this hash.
     *
     * @return Stream of BytesRef stored in the hash
     */
    public Stream<BytesRef> stream() {
      return StreamUtils.stream(this.hash);
    }
  }

  /**
   * Convert a {@link BytesRefHash} to a {@link BytesRefArray}.
   *
   * @param bh Hash
   * @return Array
   */
  public static BytesRefArray hashToArray(@NotNull final BytesRefHash bh) {
    final BytesRefArray ba = new BytesRefArray(Counter.newCounter(false));
    final BytesRef br = new BytesRef();
    for (int i = bh.size() - 1; i >= 0; i--) {
      ba.append(bh.get(i, br));
    }
    return ba;
  }

  /**
   * Convert a {@link BytesRefHash} to a Set.
   *
   * @param bh Hash
   * @return Set or {@code null}, if {@code bh} was null
   */
  @Nullable
  @Contract("null -> null; !null -> !null")
  public static Set<String> hashToSet(@Nullable final BytesRefHash bh) {
    if (bh == null) {
      return null;
    }
    final Set<String> strSet = new HashSet<>(bh.size());
    final BytesRef br = new BytesRef();
    for (int i = bh.size() - 1; i >= 0; i--) {
      strSet.add(bh.get(i, br).utf8ToString());
    }
    return strSet;
  }
}
