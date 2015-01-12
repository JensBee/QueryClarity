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

package org.mapdb;

import de.unihildesheim.iw.util.RandomValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Jens Bertram
 */
public class BTreeMapTest {
  private int[] fillIntArray(final int[] arr) {
    for (int i=0; i<arr.length; i++) {
      arr[i] = RandomValue.getInteger();
    }
    return arr;
  }

  @Test
  public void intArrayAsKey_contains() {
    final Map<int[], int[]> intArrayMap = DBMaker
        .newTempFileDB().make()
        .createTreeMap("intArrayMap")
        .keySerializerWrap(Serializer.INT_ARRAY)
        .valueSerializer(Serializer.INT_ARRAY)
        .comparator(Fun.INT_ARRAY_COMPARATOR)
        .make();
    for (int i=0; i<1000; i++) {
      final int[] key = fillIntArray(
          new int[RandomValue.getInteger(1, 10)]);
      final int[] value = fillIntArray(
          new int[RandomValue.getInteger(1, 10)]);
      intArrayMap.put(key, value);
    }
    intArrayMap.put(new int[]{1,2,3}, new int[]{4,5,6});

    final int[] sorted = new int[]{1,2,3};
    final int[] unSorted = new int[]{1,2,3};
    Arrays.sort(unSorted);

    Assert.assertTrue(intArrayMap.containsKey(sorted));
    Assert.assertTrue(intArrayMap.containsKey(unSorted));
  }
}
