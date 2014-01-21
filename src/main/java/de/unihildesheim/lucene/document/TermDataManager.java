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
package de.unihildesheim.lucene.document;

import de.unihildesheim.lucene.index.IndexDataProvider;

/**
 * Wrapper around the term data storage in {@link DocumentModel}s that allows to
 * store key-value pairs with a local name prefix and easier updating of models.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TermDataManager {

  /**
   * Prefix to use for storing data keys.
   */
  private final String prefix;

  /**
   * IndexDataProvider to use for updating the document models.
   */
  private final IndexDataProvider dataProv;

  /**
   * Creates a new {@link TermDataManager} with the provided prefix and
   * {@link IndexDataProvider}.
   *
   * @param newPrefix Prefix to use for storing data keys
   * @param newDataProv Data provider to access stored models
   */
  public TermDataManager(final String newPrefix,
          final IndexDataProvider newDataProv) {
    this.prefix = newPrefix;
    this.dataProv = newDataProv;
  }

  /**
   * Set a term data value for a given {@link DocumentModel}.
   *
   * @param docModel DocumentModel to update
   * @param term Term whose key-value pair should be updated
   * @param key Data-key
   * @param value Value to store
   * @see DocumentModel#addTermData(String, String, Object)
   */
  public void setTermData(final DocumentModel docModel, final String term,
          final String key, final Number value) {
    final DocumentModel changedModel = docModel.addTermData(term, this.prefix
            + "_" + key, value);
    this.dataProv.addDocumentModel(changedModel);
  }

  /**
   * Generic get-method for stored term-data.
   *
   * @param docModel Document model to use
   * @param term Term whose data should be retrieved
   * @param key Key to lookup
   * @return The value stored under the given key for the specific term, or
   * null, if no value was stored
   * @see DocumentModel#getTermData(String, String)
   */
  public Number getTermData(final DocumentModel docModel,
          final String term, final String key) {
    return docModel.getTermData(term, this.prefix + "_" + key);
  }
}
