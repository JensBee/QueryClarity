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

import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;

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
   * @throws de.unihildesheim.iw.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public static Collection<String> getFields()
      throws Environment.NoIndexException {
    return MultiFields.getIndexedFields(Environment.getIndexReader());
  }

  /**
   * Check if all given fields are available in the current index. Uses the
   * {@link IndexReader} provided by the {@link Environment}. Throws an {@link
   * IllegalStateException} if not all fields are present in the index.
   *
   * @param fields Fields to check
   * @throws de.unihildesheim.iw.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public static void checkFields(final String[] fields)
      throws Environment.NoIndexException {
    checkFields(Environment.getIndexReader(), fields);
  }

  /**
   * Check if all given fields are available in the current index. Throws an
   * {@link IllegalStateException} if not all fields are present in the index.
   *
   * @param reader IndexReader to use
   * @param fields Fields to check
   * @throws de.unihildesheim.iw.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public static void checkFields(final IndexReader reader,
      final String[] fields)
      throws Environment.NoIndexException {
    if (fields == null || fields.length == 0) {
      throw new IllegalArgumentException("No fields specified.");
    }

    // get all indexed fields from index - other fields are not of
    // interest here
    final Collection<String> indexedFields;
    if (reader == null) {
      indexedFields = MultiFields.getIndexedFields(
          Environment.getIndexReader());
    } else {
      indexedFields = MultiFields.getIndexedFields(
          reader);
    }

    // check if all requested fields are available
    if (!indexedFields.containsAll(Arrays.asList(fields))) {
      throw new IllegalStateException(MessageFormat.format(
          "Not all requested fields ({0}) are available in the " +
          "current index ({1}) or are not indexed.",
          StringUtils.join(fields, ","),
          Arrays.toString(indexedFields.toArray(
              new String[indexedFields.size()]
          ))
      ));
    }
  }
}
