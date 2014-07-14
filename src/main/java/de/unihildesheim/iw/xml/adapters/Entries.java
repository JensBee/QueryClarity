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

import de.unihildesheim.iw.Tuple;

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
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class Tuple2ListEntry {
    /**
     * Entry key.
     */
    @SuppressWarnings("PackageVisibleField")
    @XmlAttribute
    String key;

    /**
     * Tuple2 entries.
     */
    @SuppressWarnings("PackageVisibleField")
    List<Tuple.Tuple2<String, String>> t2List = new ArrayList<>();

    /**
     * Default constructor used for JAXB (un)marshalling.
     */
    public Tuple2ListEntry() {
    }

    /**
     * Create a new list entry with the provided key and entries.
     *
     * @param newKey Key for this list
     * @param newT2List Entries
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    Tuple2ListEntry(final String newKey, final List<Tuple.Tuple2<String,
        String>> newT2List) {
      this.key = newKey;
      this.t2List = newT2List;
    }

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    @XmlElement(name = "entries")
    @XmlJavaTypeAdapter(ListAdapter.Tuple2ListValue.class)
    List<Tuple.Tuple2<String, String>> getList() {
      return this.t2List;
    }
  }

  @SuppressWarnings("PackageVisibleInnerClass")
  static final class StringValueEntry {
    /**
     * Entry key.
     */
    @SuppressWarnings("PackageVisibleField")
    @XmlAttribute
    String key;
    /**
     * Entry value.
     */
    @SuppressWarnings("PackageVisibleField")
    @XmlValue
    String value;

    /**
     * Default constructor used for JAXB (un)marshalling.
     */
    public StringValueEntry() {
    }

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
  }
}
