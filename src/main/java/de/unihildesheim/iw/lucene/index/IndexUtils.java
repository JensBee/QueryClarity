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

import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader
    .FilteredLeafReader;
import de.unihildesheim.iw.lucene.search.FDRDefaultSimilarity;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * Simple utilities for working with a Lucene index.
 *
 * @author Jens Bertram
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
   * @param reader Reader to access Lucene index
   * @return Fields list
   */
  public static Collection<String> getFields(
      @NotNull final IndexReader reader) {
    return MultiFields.getIndexedFields(reader);
  }

  /**
   * Check if all given fields are available in the current index. Throws an
   * {@link IllegalStateException} if not all fields are present in the index.
   *
   * @param reader IndexReader to use
   * @param fields Fields to check
   */
  public static void checkFields(
      @NotNull final IndexReader reader,
      @NotNull final Set<String> fields) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("No fields specified.");
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

  /**
   * Get an {@link IndexSearcher} instance. If the reader is an instance of
   * {@link FilteredDirectoryReader} the {@link FDRDefaultSimilarity}
   * will be used.
   * @param reader Reader to use
   * @return Searcher instance
   */
  public static IndexSearcher getSearcher(final IndexReader reader) {
    final IndexSearcher searcher = new IndexSearcher(reader);
    if (FilteredDirectoryReader.class.isInstance(reader) ||
        FilteredLeafReader.class.isInstance(reader)) {
      searcher.setSimilarity(new FDRDefaultSimilarity());
    }
    return searcher;
  }
}
