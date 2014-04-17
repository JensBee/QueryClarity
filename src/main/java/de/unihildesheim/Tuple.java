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
package de.unihildesheim;

import java.io.Serializable;
import java.util.Objects;

/**
 * Tuple objects.
 *
 * @author Jens Bertram
 */
public final class Tuple {

  /**
   * Private constructor for utility class.
   */
  private Tuple() {
    // empty private constructor for utility class.
  }

  /**
   * Creates a new two-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   * @param a Value for <tt>Tuple.a</tt>
   * @param b Value for <tt>Tuple.b</tt>
   * @return New tuple
   */
  public static <A, B> Tuple2<A, B> tuple2(final A a, final B b) {
    return new Tuple2<>(a, b);
  }

  /**
   * Creates a new three-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   * @param <C> Type of <tt>Tuple.c</tt>
   * @param a Value for <tt>Tuple.a</tt>
   * @param b Value for <tt>Tuple.b</tt>
   * @param c Value for <tt>Tuple.c</tt>
   * @return New tuple
   */
  public static <A, B, C> Tuple3<A, B, C> tuple3(final A a, final B b,
          final C c) {
    return new Tuple3<>(a, b, c);
  }

  /**
   * Creates a new three-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   * @param <C> Type of <tt>Tuple.c</tt>
   * @param <D> Type of <tt>Tuple.d</tt>
   * @param a Value for <tt>Tuple.a</tt>
   * @param b Value for <tt>Tuple.b</tt>
   * @param c Value for <tt>Tuple.c</tt>
   * @param d Value for <tt>Tuple.d</tt>
   * @return New tuple
   */
  public static <A, B, C, D> Tuple4<A, B, C, D> tuple4(final A a, final B b,
          final C c, final D d) {
    return new Tuple4<>(a, b, c, d);
  }

  /**
   * Two-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Tuple2<A, B> implements Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 6255704054326088455L;

    /**
     * First object.
     */
    public final A a;
    /**
     * Second object.
     */
    public final B b;

    /**
     * Two-value tuple.
     *
     * @param newA First object
     * @param newB Second object
     */
    public Tuple2(final A newA, final B newB) {
      this.a = newA;
      this.b = newB;
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
      return !(b == null ? tuple2.b != null : !b.equals(tuple2.b));
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 47 * hash + Objects.hashCode(this.a);
      hash = 47 * hash + Objects.hashCode(this.b);
      return hash;
    }
  }

  /**
   * Three-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   * @param <C> Type of <tt>Tuple.c</tt>
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Tuple3<A, B, C> implements Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -1188331062235243640L;

    /**
     * First object.
     */
    public final A a;
    /**
     * Second object.
     */
    public final B b;
    /**
     * Third object.
     */
    public final C c;

    /**
     * Three-value tuple.
     *
     * @param newA First object
     * @param newB Second object
     * @param newC Third object
     */
    public Tuple3(final A newA, final B newB, final C newC) {
      this.a = newA;
      this.b = newB;
      this.c = newC;
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
      return !(c == null ? tuple3.c != null : !c.equals(tuple3.c));
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

  /**
   * Four-value tuple.
   *
   * @param <A> Type of <tt>Tuple.a</tt>
   * @param <B> Type of <tt>Tuple.b</tt>
   * @param <C> Type of <tt>Tuple.c</tt>
   * @param <D> Type of <tt>Tuple.d</tt>
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Tuple4<A, B, C, D> implements Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 4744843616749888999L;

    /**
     * First object.
     */
    public final A a;
    /**
     * Second object.
     */
    public final B b;
    /**
     * Third object.
     */
    public final C c;
    /**
     * Fourth object.
     */
    public final D d;

    /**
     * Three-value tuple.
     *
     * @param newA First object
     * @param newB Second object
     * @param newC Third object
     * @param newD Third object
     */
    public Tuple4(final A newA, final B newB, final C newC, final D newD) {
      this.a = newA;
      this.b = newB;
      this.c = newC;
      this.d = newD;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Tuple4 tuple4 = (Tuple4) o;

      if (a == null ? tuple4.a != null : !a.equals(tuple4.a)) {
        return false;
      }
      if (b == null ? tuple4.b != null : !b.equals(tuple4.b)) {
        return false;
      }
      if (c == null ? tuple4.c != null : !c.equals(tuple4.c)) {
        return false;
      }
      return !(d == null ? tuple4.d != null : !d.equals(tuple4.d));
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 79 * hash + Objects.hashCode(this.a);
      hash = 79 * hash + Objects.hashCode(this.b);
      hash = 79 * hash + Objects.hashCode(this.c);
      hash = 79 * hash + Objects.hashCode(this.d);
      return hash;
    }
  }
}
