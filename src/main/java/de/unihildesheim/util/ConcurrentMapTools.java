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

import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class ConcurrentMapTools {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          ConcurrentMapTools.class);

  /**
   * Private constructor for utility class.
   */
  private ConcurrentMapTools() {
    // empty constructor for utility class
  }

  /**
   * Ensure that the desired data was written to the {@link ConcurrentMap}.
   *
   * @param <K> Map key type
   * @param <V> Map value type
   * @param map Map to add to
   * @param key Key
   * @param value Value
   * @return Previously set value or null, if there was none
   */
  public static <K, V> V ensurePut(final ConcurrentMap<K, V> map, final K key,
          final V value) {
    if (key == null || value == null || map == null) {
      throw new IllegalArgumentException("Null not allowed for any value.");
    }

    V oldValue = null;
    try {
      oldValue = map.get(key);

      if (oldValue != null && oldValue.equals(value)) {
        // nothing changed
        return oldValue;
      }

      for (;;) {
        oldValue = map.putIfAbsent(key, value);
        if (oldValue == null) {
          // success, data was not already stored
          return null;
        }

        if (map.replace(key, oldValue, value)) {
          // replacing actually worked
          return oldValue;
        }
      }
    } catch (Exception ex) {
      LOG.error("Caught exception while updating map. "
              + "key={} value={} oldValue={}",
              key, value, oldValue, ex);
      throw ex;
    }
  }
}
