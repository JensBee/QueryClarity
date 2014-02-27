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

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Tuple {

  /**
   * Private constructor for utility class.
   */
  private Tuple() {
    // empty private constructor for utility class.
  }

  /**
   * Returns an object providing an equals method comparing against the first
   * two elements of a {@link Tuple}.
   * <p>
   * If an object is <tt>null</tt> it will not be tested. At least one object
   * must be not <tt>null</tt>.
   *
   * @param o1 Object to match against <tt>Tuple.a</tt>
   * @param o2 Object to match against <tt>Tuple.b</tt>
   * @return The matcher object
   */
  public static Object tupleMatcher(final Object o1, final Object o2) {
    return new Tuple2Matcher(o1, o2);
  }

  /**
   * Returns an object providing an equals method comparing against the first
   * three elements of a {@link Tuple}.
   * <p>
   * If an object is <tt>null</tt> it will not be tested. At least one object
   * must be not <tt>null</tt>.
   *
   * @param o1 Object to match against <tt>Tuple.a</tt>
   * @param o2 Object to match against <tt>Tuple.b</tt>
   * @param o3 Object to match against <tt>Tuple.c</tt>
   * @return The matcher object
   */
  public static Object tupleMatcher(final Object o1, final Object o2,
          final Object o3) {
    return new Tuple3Matcher(o1, o2, o3);
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
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

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
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

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
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

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
      if (d == null ? tuple4.d != null : !d.equals(tuple4.d)) {
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
      hash = 79 * hash + Objects.hashCode(this.d);
      return hash;
    }
  }

  /**
   * Object providing an equals method comparing against the first three
   * elements of a {@link Tuple}.
   * <p>
   * If an object is <tt>null</tt> it will not be tested. At least one object
   * must be not <tt>null</tt>.
   */
  @SuppressWarnings({"PublicInnerClass", "EqualsAndHashcode"})
  public static final class Tuple2Matcher {

    /**
     * Object to match <tt>Tuple.a</tt>.
     */
    private final Object o1;
    /**
     * Object to match <tt>Tuple.b</tt>.
     */
    private final Object o2;

    /**
     * Match against at most two objects. One of the objects may be
     * <tt>null</tt> to act as wildcard.
     *
     * @param obj1 Object to match <tt>Tuple.a</tt>
     * @param obj2 Object to match <tt>Tuple.b</tt>
     */
    Tuple2Matcher(final Object obj1, final Object obj2) {
      if (obj1 == null && obj2 == null) {
        throw new IllegalArgumentException("Both parameters were null.");
      }
      this.o1 = obj1;
      this.o2 = obj2;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == null || Tuple2.class != o.getClass()) {
        return false;
      }

      Tuple2 t = (Tuple2) o;

      if (o1 != null && !o1.equals(t.a)) {
        return false;
      }
      if (o2 != null && !o2.equals(t.b)) {
        return false;
      }

      return true;
    }
  }

  /**
   * Object providing an equals method comparing against the first two elements
   * of a {@link Tuple}.
   * <p>
   * If an object is <tt>null</tt> it will not be tested. At least one object
   * must be not <tt>null</tt>.
   */
  @SuppressWarnings({"PublicInnerClass", "EqualsAndHashcode"})
  public static final class Tuple3Matcher {

    /**
     * Object to match <tt>Tuple.a</tt>.
     */
    private final Object o1;
    /**
     * Object to match <tt>Tuple.b</tt>.
     */
    private final Object o2;
    /**
     * Object to match <tt>Tuple.c</tt>.
     */
    private final Object o3;

    /**
     * Match against at most three objects. Two of the objects may be
     * <tt>null</tt> to act as wildcard.
     *
     * @param obj1 Object to match <tt>Tuple.a</tt>
     * @param obj2 Object to match <tt>Tuple.b</tt>
     * @param obj3 Object to match <tt>Tuple.c</tt>
     */
    Tuple3Matcher(final Object obj1, final Object obj2, final Object obj3) {
      if (obj1 == null && obj2 == null && obj3 == null) {
        throw new IllegalArgumentException("All three parameters were null.");
      }
      this.o1 = obj1;
      this.o2 = obj2;
      this.o3 = obj3;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == null || Tuple3.class != o.getClass()) {
        return false;
      }

      Tuple3 t = (Tuple3) o;

      if (o1 != null && !o1.equals(t.a)) {
        return false;
      }
      if (o2 != null && !o2.equals(t.b)) {
        return false;
      }
      if (o3 != null && !o3.equals(t.c)) {
        return false;
      }

      return true;
    }
  }
}
