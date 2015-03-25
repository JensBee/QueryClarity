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

package de.unihildesheim.iw.xml.adapters;

import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.xml.adapters.ListAdapter.Tuple2ListValue;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.ArrayList;
import java.util.List;

/**
 * XML element types used by adapters.
 *
 * @author Jens Bertram
 */
final class Entries {
  /**
   * List of tuple2 values.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class Tuple2ListEntry {
    /**
     * Entry key.
     */
    @XmlAttribute
    String key;

    /**
     * Tuple2 entries.
     */
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    @XmlElement
    @XmlJavaTypeAdapter(Tuple2ListValue.class)
    List<Tuple2<String, String>> entries = new ArrayList<>();

    /**
     * Create a new list entry with the provided key and entries.
     *
     * @param newKey Key for this list
     * @param newT2List Entries
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    Tuple2ListEntry(final String newKey, final List<Tuple2<String,
        String>> newT2List) {
      this.key = newKey;
      this.entries = newT2List;
    }

    /**
     * Get the list of tuple2 values.
     * @return List of values
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    List<Tuple2<String, String>> getEntries() {
      return this.entries;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(this.entries.size() * 100);
      sb.append("Tuple2ListEntry(").append(this.key).append("){")
          .append(this.entries.size()).append(':');
      for (final Tuple2<String, String> t2 : this.entries) {
        sb.append("[a=").append(t2.a).append(" b=").append(t2.b).append(']');
      }
      return sb.append('}').toString();
    }
  }

  /**
   * Simple string value entry.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class StringValueEntry {
    /**
     * Entry key.
     */
    @XmlAttribute
    String key;
    /**
     * Entry value.
     */
    @XmlValue
    String value;

    /**
     * Create a new entry with a string value.
     *
     * @param newKey Entry key
     * @param newValue Entry value
     */
    StringValueEntry(final String newKey, final String newValue) {
      this.key = newKey;
      this.value = newValue;
    }

    @Override
    public String toString() {
      return "StringValueEntry{k=" + this.key + " v=" + this.value + '}';
    }
  }
}
