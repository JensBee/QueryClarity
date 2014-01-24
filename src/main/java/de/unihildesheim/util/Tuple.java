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
import java.util.Objects;
import org.mapdb.Fun;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Tuple {

  /**
   * Private constructor for utility class.
   */
  private Tuple() {
    // empty private constructor for utility class.
  }

  public static <A, B> Tuple2<A, B> tuple2(A a, B b) {
    return new Tuple2<A, B>(a, b);
  }

  public static <A, B, C> Tuple3<A, B, C> tuple3(A a, B b, C c) {
    return new Tuple3<A, B, C>(a, b, c);
  }

  public static final class Tuple2<A, B> implements Serializable {

    /**
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

    public final A a;
    public final B b;

    public Tuple2(final A a, final B b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Tuple2 tuple2 = (Tuple2) o;

      if (a == null ? tuple2.a != null : !a.equals(tuple2.a)) {
        return false;
      }
      if (b == null ? tuple2.b != null : !b.equals(tuple2.b)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 47 * hash + Objects.hashCode(this.a);
      hash = 47 * hash + Objects.hashCode(this.b);
      return hash;
    }
  }

  public static final class Tuple3<A, B, C> implements Serializable {

    /**
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

    public final A a;
    public final B b;
    public final C c;

    public Tuple3(final A a, final B b, final C c) {
      this.a = a;
      this.b = b;
      this.c = c;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Tuple3 tuple3 = (Tuple3) o;

      if (a == null ? tuple3.a != null : !a.equals(tuple3.a)) {
        return false;
      }
      if (b == null ? tuple3.b != null : !b.equals(tuple3.b)) {
        return false;
      }
      if (c == null ? tuple3.c != null : !c.equals(tuple3.c)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 79 * hash + Objects.hashCode(this.a);
      hash = 79 * hash + Objects.hashCode(this.b);
      hash = 79 * hash + Objects.hashCode(this.c);
      return hash;
    }
  }
}
