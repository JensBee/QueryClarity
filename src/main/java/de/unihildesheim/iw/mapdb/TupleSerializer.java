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

package de.unihildesheim.iw.mapdb;

import org.mapdb.Fun;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Jens Bertram
 */
public class TupleSerializer {
  public static class Tuple3Serializer<A, B, C>
      implements Serializer<Fun.Tuple3<A, B, C>>, Serializable {

    private final Serializer<A> aSerializer;
    private final Serializer<B> bSerializer;
    private final Serializer<C> cSerializer;

    public Tuple3Serializer(Serializer<A> newASerializer,
        Serializer<B> newBSerializer, Serializer<C> newCSerializer) {
      this.aSerializer = newASerializer;
      this.bSerializer = newBSerializer;
      this.cSerializer = newCSerializer;
    }

    @Override
    public void serialize(final DataOutput out, final Fun.Tuple3<A, B, C> value)
        throws IOException {
      this.aSerializer.serialize(out, value.a);
      this.bSerializer.serialize(out, value.b);
      this.cSerializer.serialize(out, value.c);
    }

    @Override
    public Fun.Tuple3<A, B, C> deserialize(final DataInput in,
        final int available)
        throws IOException {
      return Fun.t3(
          this.aSerializer.deserialize(in, available),
          this.bSerializer.deserialize(in, available),
          this.cSerializer.deserialize(in, available)
      );
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }
}
