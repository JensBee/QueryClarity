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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches document model instances for faster access. This is currently a simple
 * wrapper around a {@link ConcurrentHashMap} instance.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelPool {

  /**
   * Size of the pool.
   */
  private final int size;

  /**
   * Backing map store.
   */
  private Map<Integer, DocumentModel> map;

  /**
   * Creates a new {@link DocumentModel} pool of the given initial size.
   *
   * @param newSize Size of the pool
   */
  public DocumentModelPool(final int newSize) {
    this.map = new ConcurrentHashMap(newSize);
    this.size = newSize;
  }

  /**
   * Test, if a document with the given id is in the pool.
   *
   * @param docId Document-id to lookup
   * @return True, if it present
   */
  public boolean containsDocId(final Integer docId) {
    return this.map.containsKey(docId);
  }

  /**
   * Get the capacity (maximum size) of this pool.
   *
   * @return Size of the pool
   */
  public int capacity() {
    return this.size;
  }

  /**
   * Get the current size of the pool.
   *
   * @return Pool size
   */
  public int size() {
    return this.map.size();
  }

  /**
   * Check if the pool is empty
   *
   * @return True, if pool is empty
   */
  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  /**
   * Get a set of entries stored in the pool.
   *
   * @return Pool entries
   */
  public Set<Entry<Integer, DocumentModel>> entrySet() {
    return this.map.entrySet();
  }

  /**
   * Add a model to the pool.
   *
   * @param docModel Model to add
   */
  public void put(final DocumentModel docModel) {
    if (docModel == null) {
      throw new NullPointerException();
    }
    this.map.put(docModel.getDocId(), docModel);
  }

  /**
   * Removes a model from the pool by it's document-id.
   *
   * @param docId Document-id of the model to remove
   * @return Previously assigned model, or <tt>null</tt> if there was none
   */
  public DocumentModel remove(final int docId) {
    return this.map.remove(docId);
  }

  /**
   * Tries to get a {@link DocumentModel} from the current pool state. After
   * returning it's not guaranteed that the returned model is still part of the
   * pool.
   *
   * @param docId Document-id to lookup
   * @return Document-model for the given document-id or null, if none found
   */
  public DocumentModel get(final Integer docId) {
    return this.map.get(docId);
  }
}
