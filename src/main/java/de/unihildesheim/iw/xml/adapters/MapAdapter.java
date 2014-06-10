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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jens Bertram
 */
public class MapAdapter {

  /**
   * XML processing of mapping from key to Tuple2.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class Tuple2ListValue
      extends XmlAdapter<Entries.Tuple2ListEntry[], Map<String,
      List<Tuple.Tuple2<String, String>>>> {

    @SuppressWarnings("ReturnOfNull")
    @Override
    public Map<String, List<Tuple.Tuple2<String, String>>> unmarshal(
        final Entries.Tuple2ListEntry[] value)
        throws Exception {
      if (null == value) {
        return null;
      }
      final Map<String, List<Tuple.Tuple2<String, String>>> retMap =
          new HashMap<>(value.length);
      for (final Entries.Tuple2ListEntry t2Val : value) {
        retMap.put(t2Val.key, t2Val.t2List);
      }
      return retMap;
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public Entries.Tuple2ListEntry[] marshal(
        final Map<String, List<Tuple.Tuple2<String, String>>> value)
        throws Exception {
      final Entries.Tuple2ListEntry[] mapElements =
          new Entries.Tuple2ListEntry[value.size()];

      int i = 0;
      for (final Map.Entry<String, List<Tuple.Tuple2<String,
          String>>> t2Val : value.entrySet()) {
        mapElements[i++] =
            new Entries.Tuple2ListEntry(t2Val.getKey(), t2Val.getValue());
      }
      return mapElements;
    }
  }

  /**
   * XML processing of mapping from key to String.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class StringValue
      extends XmlAdapter<Entries.StringValueEntry[], Map<String, String>> {

    @SuppressWarnings("ReturnOfNull")
    @Override
    public Map<String, String> unmarshal(final Entries.StringValueEntry[] value)
        throws Exception {
      if (null == value) {
        return null;
      }
      final Map<String, String> retMap = new HashMap<>(value.length);
      for (final Entries.StringValueEntry sVal : value) {
        retMap.put(sVal.key, sVal.value);
      }
      return retMap;
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public Entries.StringValueEntry[] marshal(final Map<String, String> value)
        throws Exception {
      final Entries.StringValueEntry[] mapElements =
          new Entries.StringValueEntry[value.size()];

      int i = 0;
      for (final Map.Entry<String, String> sVal : value.entrySet()) {
        mapElements[i++] =
            new Entries.StringValueEntry(sVal.getKey(), sVal.getValue());
      }
      return mapElements;
    }
  }
}
