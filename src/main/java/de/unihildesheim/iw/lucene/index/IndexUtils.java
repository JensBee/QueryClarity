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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
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
   * @return Fields list
   */
  public static Collection<String> getFields(final IndexReader reader) {
    Objects.requireNonNull(reader, "IndexReader was null.");
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
    Objects.requireNonNull(reader, "IndexReader was null.");
    if (Objects.requireNonNull(fields, "Fields were null.").isEmpty()) {
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
   * Get an {@link IndexReader} for a file based Lucene index.
   *
   * @param idxDir Directory where the Lucene index is located at
   * @return Reader for the given index
   * @throws IOException Thrown, if the index is not found or any other
   * low-level I/O error occurred
   */
  public static IndexReader openReader(final File idxDir)
      throws IOException {
    // check, if there's a Lucene index in the path
    final FSDirectory luceneDir = FSDirectory.open(idxDir);
    if (!DirectoryReader.indexExists(luceneDir)) {
      throw new IOException("No index found at index path '" + idxDir
          .getCanonicalPath() + "'.");
    }
    return DirectoryReader.open(luceneDir);
  }
}
