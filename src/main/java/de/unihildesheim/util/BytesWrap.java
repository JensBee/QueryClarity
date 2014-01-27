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
package de.unihildesheim.util;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Based on: https://stackoverflow.com/a/1058169
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class BytesWrap implements Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Copy of the original byte array passed at construction time.
   */
  private byte[] data;

  private boolean isDuplicated = false;

  /**
   * Pre-calculated hash code of this instance. A hash code of 0 means that the
   * bytes of this instance are referenced and a hash code must be calculated
   * ad-hoc.
   */
  private int hash = 0;

  public BytesWrap(byte[] existingData, final boolean duplicate) {
    if (existingData == null) {
      throw new IllegalArgumentException("Byte array was null.");
    }
    if (duplicate) {
      this.data = existingData.clone(); // local copy - to be immutable
      this.hash = Arrays.hashCode(data); // pre-calc hash-code
      this.isDuplicated = true;
    } else {
      this.data = existingData.clone(); // ref only - mutable
    }
  }

  /**
   * Creates a new {@link BytesWrap} instance by referencing the given array.
   * This means any changes to the passed in array will be reflected by this
   * instance.
   *
   * @param existingBytes Array to reference
   * @return Instance with the referenced array set
   */
  public static BytesWrap wrap(final byte[] existingBytes) {
    return new BytesWrap(existingBytes, false);
  }

  /**
   * Creates a new {@link BytesWrap} instance by duplicating (making a local
   * copy) of the given array. This means any changes to the passed in array
   * will <b>not</b> be reflected by this instance.
   *
   * @param bytesToClone Array to create a copy of
   * @return Instance with the copy of the given array set
   */
  public static BytesWrap duplicate(final byte[] bytesToClone) {
    return new BytesWrap(bytesToClone, true);
  }

  /**
   * Create a local copy of the referenced byte array. This will do nothing, if
   * the copying was already done.
   *
   * @return Self reference
   */
  public BytesWrap duplicate() {
    if (!this.isDuplicated) {
      this.data = this.data.clone();
      this.hash = Arrays.hashCode(data);
      this.isDuplicated = true;
    }
    return this;
  }

  /**
   * Returns a reference to the internal used or referenced byte array. Meant to
   * be used in serialization.
   *
   * @return Internal used or referenced byte array
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected byte[] getBytes() {
    return this.data;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof BytesWrap)) {
      return false;
    }
    return Arrays.equals(data, ((BytesWrap) o).data);
  }

  @Override
  public int hashCode() {
    return this.isDuplicated? this.hash : Arrays.hashCode(data);
  }
}
