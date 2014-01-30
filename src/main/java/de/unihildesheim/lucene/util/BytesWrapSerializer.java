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
package de.unihildesheim.lucene.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.mapdb.Serializer;

/**
 * Custom {@link Serializer} for {@link BytesWrap} objects.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class BytesWrapSerializer implements Serializer<BytesWrap>,
        Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  @Override
  public void serialize(final DataOutput out, final BytesWrap value) throws
          IOException {
    final byte[] bytes = value.getBytes();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public BytesWrap deserialize(final DataInput in, final int available) throws
          IOException {
    if (available == 0) {
      return null;
    }
    final byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new BytesWrap(bytes, false);
  }

}
