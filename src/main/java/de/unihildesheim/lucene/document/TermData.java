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
package de.unihildesheim.lucene.document;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map.Entry;

/**
 * Simple triple object to store data associated to a term by specifying key and
 * value pairs.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 * @param <K> Key type
 * @param <V> Value type
 */
public final class TermData<K, V> implements Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Term to which the data is related to.
   */
  private final String term;

  /**
   * Key to identify the data part.
   */
  private final K key;

  /**
   * Data to store under a specific key.
   */
  private final V value;

  /**
   * Creates a new {@link TermData} triple.
   *
   * @param newTerm Term to which the data is related to
   * @param newKey Key to identify the data part
   * @param newValue Data to store under a specific key
   */
  public TermData(final String newTerm, final K newKey, final V newValue) {
    this.term = newTerm;
    this.key = newKey;
    this.value = newValue;
  }

  /**
   * Get the term part of this triple.
   *
   * @return Term value
   */
  public String getTerm() {
    return this.term;
  }

  /**
   * Get the key part of this triple.
   *
   * @return Key object
   */
  public K getKey() {
    return this.key;
  }

  /**
   * Get the value part of this triple.
   *
   * @return Stored value
   */
  public V getValue() {
    return this.value;
  }

  /**
   * Get the <tt>key, value</tt> touple stored in this triple.
   *
   * @return <tt>key, value</tt> touple stored in this triple
   */
  public Entry<K, V> getEntry() {
    return new AbstractMap.SimpleEntry(getKey(), getValue());
  }
}
