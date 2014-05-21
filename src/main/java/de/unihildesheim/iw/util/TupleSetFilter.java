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

package de.unihildesheim.iw.util;

import org.mapdb.Fun;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Additional filters for {@link org.mapdb.Fun} {@code Tuple} objects. Extended
 * as required.
 *
 * @author Jens Bertram
 */
public class TupleSetFilter {

  /**
   * Filter {@code B} value from a {@link Fun.Tuple3}.
   *
   * @param secondaryKeys
   * @param a Required value of {@code Tuple.a}
   * @param c Upper bound or fixed value for {@code Tuple.c}. May be null, to
   * allow all values for {@code c}
   * @param <A> Type of {@code Tuple.a}
   * @param <B> Type of {@code Tuple.b}
   * @param <C> Type of {@code Tuple.c}
   * @return Iterator over {@code Tuple.b} for all items with {@code Tuple.a}
   * matching {@code a} and {@code Tuple.c} matching {@code c}
   */
  public static <A, B, C> Iterable<B> filterB(
      final NavigableSet<Fun.Tuple3<A, B, C>> secondaryKeys,
      final A a, final C c) {
    return new Iterable<B>() {
      @Override
      public Iterator<B> iterator() {
        //use range query to get all values
        final Iterator<Fun.Tuple3> iter =
            ((NavigableSet) secondaryKeys) //cast is workaround for generics
                .subSet(
                    // NULL represents lower bound,
                    // everything is larger than null
                    Fun.t3(a, null, c),
                    // HI is upper bound everything is smaller then HI
                    Fun.t3(a, Fun.HI(), c == null ? Fun.HI() : c)
                ).iterator();

        return new Iterator<B>() {
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public B next() {
            return (B) iter.next().b;
          }

          @Override
          public void remove() {
            iter.remove();
          }
        };
      }
    };

  }
}
