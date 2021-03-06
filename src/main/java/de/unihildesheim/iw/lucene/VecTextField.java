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
package de.unihildesheim.iw.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

/**
 * Based on https://stackoverflow.com/a/11963832
 */
public final class VecTextField
    extends Field {

  /**
   * Indexed, tokenized, not stored.
   */
  private static final FieldType TYPE_NOT_STORED = new FieldType();

  /**
   * Indexed, tokenized, stored.
   */
  private static final FieldType TYPE_STORED = new FieldType();

  static {
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TYPE_NOT_STORED.setTokenized(true);
    TYPE_NOT_STORED.setStoreTermVectors(true);
    TYPE_NOT_STORED.setStoreTermVectorPositions(true);
    TYPE_NOT_STORED.freeze();

    TYPE_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TYPE_STORED.setTokenized(true);
    TYPE_STORED.setStored(true);
    TYPE_STORED.setStoreTermVectors(true);
    TYPE_STORED.setStoreTermVectorPositions(true);
    TYPE_STORED.freeze();
  }

  /**
   * Creates a new TextField with String value.
   *
   * @param aName Fields name
   * @param value Value
   * @param store Storage configuration
   */
  public VecTextField(final String aName, final String value,
      final Store store) {
    super(aName, value, store == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
  }
}
