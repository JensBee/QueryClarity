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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Internal {@link Map} to cache values. This map can be used for caching
 * values. If the map exceeds the maximum size then the eldest entries will
 * get removed from the map.
 *
 * @param <K> Key type
 * @param <V> Value type
 
 */
public final class InternMap<K, V> extends LinkedHashMap<K, V> {

  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -5027772503949664437L;

  /**
   * Maximum size of the map.
   */
  private final int maxSize;

  /**
   * Creates a new map with the given maximum size. Eldest elements will be
   * removed if maximum map size gets reached.
   *
   * @param newMaxSize Maximum size of the map
   */
  public InternMap(final int newMaxSize) {
    this.maxSize = newMaxSize;
  }

  @Override
  protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
    return size() > maxSize;
  }
}
