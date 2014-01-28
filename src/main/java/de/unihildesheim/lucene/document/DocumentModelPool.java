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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Caches document model instances for faster access.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelPool extends ArrayBlockingQueue<DocumentModel> {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Cache ids of documents in the pool.
   */
  private final List<Integer> docIds;
  /**
   * Size of the pool.
   */
  private final int size;

  /**
   * Creates a new {@link DocumentModel} pool of the given size. If the pool
   * gets filled then (oldest model first) older entries will be removed.
   *
   * @param newSize Size of the pool
   */
  public DocumentModelPool(final int newSize) {
    super(newSize);
    this.size = newSize;
    this.docIds = new ArrayList(newSize);
  }

  /**
   * Test, if a document with the given id is in the pool.
   *
   * @param docId Document-id to lookup
   * @return True, if it present
   */
  public boolean containsDocId(final Integer docId) {
    return this.docIds.contains(docId);
  }

  public int capacity() {
    return this.size;
  }

  public DocumentModel get(final Integer docId) {
    final Iterator<DocumentModel> dmIt = super.iterator();
    DocumentModel dm = null;
    while (dmIt.hasNext()) {
      dm = dmIt.next();
      if (dm.getDocId() == docId) {
        break;
      }
    }
    return dm;
  }

  @Override
  public DocumentModel take() throws InterruptedException {
    final DocumentModel docModel = super.take();
    this.docIds.remove(Integer.valueOf(docModel.getDocId()));
    return docModel;
  }

  /**
   * {@inheritDoc} New elements will only be added, if they're not already
   * present.
   *
   * @param elem Document model to add to the pool
   * @throws java.lang.InterruptedException Thrown, if thread was interrupted
   */
  @Override
  public void put(final DocumentModel elem) throws InterruptedException {
    if (!contains(elem)) {
      this.docIds.add(elem.getDocId());
      super.put(elem);
    }
  }
}
