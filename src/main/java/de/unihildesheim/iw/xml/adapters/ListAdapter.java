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

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jens Bertram
 */
public class ListAdapter {
  @SuppressWarnings("PublicInnerClass")
  public static class Tuple2ListValue
      extends
      XmlAdapter<Entries.StringValueEntry[], List<Tuple.Tuple2<String,
          String>>> {

    @Override
    public List<Tuple.Tuple2<String, String>> unmarshal(
        final Entries.StringValueEntry[] value)
        throws Exception {
      if (null == value) {
        return null;
      }
      final List<Tuple.Tuple2<String, String>> retList =
          new ArrayList<>(value.length);
      for (final Entries.StringValueEntry sVal : value) {
        retList.add(Tuple.tuple2(sVal.key, sVal.value));
      }
      return retList;
    }

    @Override
    public Entries.StringValueEntry[] marshal(
        final List<Tuple.Tuple2<String, String>> value)
        throws Exception {
      final Entries.StringValueEntry[] listElements =
          new Entries.StringValueEntry[value.size()];

      int i = 0;
      for (final Tuple.Tuple2<String, String> t2Val : value) {
        listElements[i++] =
            new Entries.StringValueEntry(t2Val.a, t2Val.b);
      }
      return listElements;
    }
  }
}
