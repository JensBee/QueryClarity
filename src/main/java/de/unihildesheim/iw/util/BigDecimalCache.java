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

import de.unihildesheim.iw.mapdb.DBMakerUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author Jens Bertram
 */
public class BigDecimalCache {
  private static final DB CACHE = DBMakerUtils.newTempFileDB().make();
  /**
   * Cache long values.
   */
  private static final Map<Long, BigDecimal> NUMBER_CACHE_L =
      CACHE.createTreeMap("cacheLong")
      .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
      .valueSerializer(Serializer.BASIC)
      .make();
  /**
   * Cache double values.
   */
  private static final Map<Double, BigDecimal> NUMBER_CACHE_D =
      CACHE.createTreeMap("cacheDouble")
          .keySerializer(BTreeKeySerializer.BASIC)
          .valueSerializer(Serializer.BASIC)
          .make();

  /**
   * Cache Multiplication results of {@link BigDecimal}s
   */
  private static final Map<Fun.Tuple2<BigDecimal, BigDecimal>,
      BigDecimal> MUL_CACHE =
      CACHE.createTreeMap("cacheMul")
          .keySerializer(BTreeKeySerializer.BASIC)
          .valueSerializer(Serializer.BASIC)
          .make();

  public static BigDecimal get(final long val) {
    BigDecimal ret = NUMBER_CACHE_L.get(val);
    if (ret == null) {
      ret = BigDecimal.valueOf(val);
      NUMBER_CACHE_L.put(val, ret);
    }
    return ret;
  }

  public static BigDecimal get(final double val) {
    BigDecimal ret = NUMBER_CACHE_D.get(val);
    if (ret == null) {
      ret = BigDecimal.valueOf(val);
      NUMBER_CACHE_D.put(val, ret);
    }
    return ret;
  }

  public static BigDecimal getMul(
      final Fun.Tuple2<BigDecimal, BigDecimal> val) {
    BigDecimal ret = MUL_CACHE.get(val);
    if (ret == null) {
      ret = val.a.multiply(val.b);
      MUL_CACHE.put(val, ret);
    }
    return ret;
  }

  public static BigDecimal getMul(
      final BigDecimal val1, final BigDecimal val2) {
    return getMul(Fun.t2(val1, val2));
  }
}
