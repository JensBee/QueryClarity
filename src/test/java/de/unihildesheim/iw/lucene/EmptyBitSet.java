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

package de.unihildesheim.iw.lucene;

import org.apache.lucene.util.BitSet;

/**
 * Simple {@link BitSet} implementation that is empty and does nothing.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class EmptyBitSet
    extends BitSet {
  public EmptyBitSet() {
  }

  @Override
  public void set(final int i) {
    // NOP
  }

  @Override
  public void clear(final int startIndex, final int endIndex) {
    // NOP
  }

  @Override
  public int cardinality() {
    return 0;
  }

  @Override
  public int prevSetBit(final int index) {
    return 0;
  }

  @Override
  public int nextSetBit(final int index) {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0L;
  }

  @Override
  public void clear(final int index) {
    // NOP
  }

  @Override
  public boolean get(final int index) {
    return false;
  }

  @Override
  public int length() {
    return 0;
  }
}
