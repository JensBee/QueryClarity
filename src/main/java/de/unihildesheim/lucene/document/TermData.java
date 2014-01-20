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

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class TermData<K, V> implements Serializable {
  final String term;
  final K key;
  final V value;

  public TermData(String term, K key, V value) {
    this.term = term;
    this.key = key;
    this.value = value;
  }

  public String getTerm() {
    return this.term;
  }

  public K getKey() {
    return this.key;
  }

  public V getValue() {
    return this.value;
  }
}
