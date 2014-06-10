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
class Entries {
  static class Tuple2ListEntry {
    @SuppressWarnings("PackageVisibleField")
    @XmlAttribute
    String key;

    @SuppressWarnings("PackageVisibleField")
    List<Tuple.Tuple2<String, String>> t2List = new ArrayList<>();

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

  static class StringValueEntry {
    @XmlAttribute
    String key;
    @XmlValue
    String value;

    StringValueEntry(final String newKey, final String newValue) {
      this.key = newKey;
      this.value = newValue;
    }
  }
}
