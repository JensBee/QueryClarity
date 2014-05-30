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

package de.unihildesheim.iw.clef.xml.elements;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jens Bertram
 */
public class Configuration {

  public static class ConfAdapter
      extends XmlAdapter<ConfElements[], Map<String, String>> {
    public Map<String, String> unmarshal(ConfElements[] arg0)
        throws Exception {
      Map<String, String> r = new HashMap<>();
      for (ConfElements mapelement : arg0) {
        r.put(mapelement.key, mapelement.value);
      }
      return r;
    }

    public ConfElements[] marshal(Map<String, String> arg0)
        throws Exception {
      ConfElements[] mapElements = new ConfElements[arg0.size()];

      int i = 0;
      for (Map.Entry<String, String> entry : arg0.entrySet()) {
        mapElements[i++] = new ConfElements(entry.getKey(), entry.getValue());
      }
      return mapElements;
    }
  }

  public static class ConfElements {
    @XmlAttribute
    public String key;
    @XmlAttribute
    public String value;

    private ConfElements() {
    } //Required by JAXB

    public ConfElements(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }
}
