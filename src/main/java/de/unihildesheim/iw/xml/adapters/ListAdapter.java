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
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.xml.adapters.Entries.StringValueEntry;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jens Bertram
 */
final class ListAdapter {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ListAdapter.class);
  /**
   * XML processing of Tuple2 objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Tuple2ListValue
      extends
      XmlAdapter<StringValueEntry[], List<Tuple2<String,
          String>>> {

    @Nullable
    @Override
    public final List<Tuple2<String, String>> unmarshal(
        final StringValueEntry[] value)
        throws Exception {
      if (null == value) {
        return null;
      }
      LOG.debug("Tuple2ListValue unmarshal {} entries.",
          value.length);
      final List<Tuple2<String, String>> retList =
          new ArrayList<>(value.length);
      for (final StringValueEntry sVal : value) {
        retList.add(Tuple.tuple2(sVal.key, sVal.value));
      }
      return retList;
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public final StringValueEntry[] marshal(
        final List<Tuple2<String, String>> value)
        throws Exception {
      final StringValueEntry[] listElements =
          new StringValueEntry[value.size()];

      int i = 0;
      for (final Tuple2<String, String> t2Val : value) {
        listElements[i++] =
            new StringValueEntry(t2Val.a, t2Val.b);
      }
      return listElements;
    }
  }
}
