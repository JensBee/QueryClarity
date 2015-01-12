/*
 * Copyright (C) 2014 bhoerdzn
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

package org.mapdb;

import java.util.Iterator;
import java.util.Queue;

/**
 * @author Jens Bertram
 */
public final class QueueUtils {

  public static <T> Iterator<T> iterator(final Queue q) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return !q.isEmpty();
      }

      @Override
      public T next() {
        return (T) q.poll();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
