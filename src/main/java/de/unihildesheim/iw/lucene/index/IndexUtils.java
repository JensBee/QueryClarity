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
package de.unihildesheim.iw.lucene.index;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * Simple utilities for working with a Lucene index.
 */
public final class IndexUtils {

  /**
   * Private constructor for utility class.
   */
  private IndexUtils() {
    // empty utility class constructor
  }

  /**
   * Get a list of fields available in the index.
   *
   * @return Fields list
   */
  public static Collection<String> getFields(final IndexReader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("IndexReader was null.");
    }
    return MultiFields.getIndexedFields(reader);
  }

  /**
   * Check if all given fields are available in the current index. Throws an
   * {@link IllegalStateException} if not all fields are present in the index.
   *
   * @param reader IndexReader to use
   * @param fields Fields to check
   */
  public static void checkFields(final IndexReader reader,
      final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("No fields specified.");
    }
    if (reader == null) {
      throw new IllegalArgumentException("IndexReader was null.");
    }

    // get all indexed fields from index - other fields are not of
    // interest here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
          reader);

    // check if all requested fields are available
    if (!indexedFields.containsAll(fields)) {
      throw new IllegalStateException(MessageFormat.format(
          "Not all requested fields ({0}) are available in the " +
          "current index ({1}) or are not indexed.",
          fields,
          Arrays.toString(indexedFields.toArray(
              new String[indexedFields.size()]
          ))
      ));
    }
  }
}
