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

package de.unihildesheim.iw.xml.elements;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Jens Bertram
 */
public final class Configuration {

  /**
   * Adapter to handle configuration element XML elements.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ConfAdapter
      extends XmlAdapter<ConfElements[], Map<String, String>> {
    /**
     * Unmarshals an array of configuration elements into a map.
     *
     * @param arg0 Configuration elements
     * @return Map of configuration elements
     */
    @Override
    public Map<String, String> unmarshal(final ConfElements[] arg0) {
      final Map<String, String> r = new HashMap<>(arg0.length);
      for (final ConfElements mapelement : arg0) {
        r.put(mapelement.key, mapelement.value);
      }
      return r;
    }

    /**
     * Marshals configuration elements into an array of elements.
     *
     * @param arg0 Map of configuration elements
     * @return Array of configuration element objects
     */
    @Override
    @SuppressWarnings("ObjectAllocationInLoop")
    public ConfElements[] marshal(final Map<String, String> arg0) {
      final ConfElements[] mapElements = new ConfElements[arg0.size()];

      int i = 0;
      for (final Entry<String, String> entry : arg0.entrySet()) {
        mapElements[i++] = new ConfElements(entry.getKey(), entry.getValue());
      }
      return mapElements;
    }
  }

  /**
   * Simple key, value configuration element.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ConfElements {
    /**
     * Element key.
     */
    @XmlAttribute
    public final String key;
    /**
     * Element value.
     */
    @XmlAttribute
    public final String value;

    /**
     * Create a new ConfElement with given key and value.
     *
     * @param newKey Key
     * @param newValue Value
     */
    public ConfElements(final String newKey, final String newValue) {
      this.key = newKey;
      this.value = newValue;
    }
  }
}
