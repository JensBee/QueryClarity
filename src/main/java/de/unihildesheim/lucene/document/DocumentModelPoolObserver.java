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
import de.unihildesheim.util.TimeMeasure;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes the pool of cached {@link DocumentModel}s to dequeue and update the
 * data store as needed.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelPoolObserver implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final transient Logger LOG = LoggerFactory.getLogger(
          DocumentModelPoolObserver.class);
  /**
   * Pool to observe.
   */
  private final DocumentModelPool modelPool;
  /**
   * Flag indicating if this instance should terminate.
   */
  private boolean terminate = false;
  /**
   * {@link IndexDataProvider} to commit changes to.
   */
  private final IndexDataProvider dataProv;
  /**
   * Externally maintained list of models being currently modified. May be null
   * if not used.
   */
  private final Set<Integer> lockedModels;

  private static final double POOL_LOAD = 0.75;

  /**
   * Creates a new observer for the given pool and queue.
   *
   * @param newDataProv Data provider to commit to
   * @param newModelQueue Queue to observe
   * @param lockedModelsSet Shared set of model currently being locked
   */
  public DocumentModelPoolObserver(final IndexDataProvider newDataProv,
          final DocumentModelPool newModelQueue,
          final Set<Integer> lockedModelsSet) {
    this.modelPool = newModelQueue;
    this.dataProv = newDataProv;
    this.lockedModels = lockedModelsSet;
    LOG.debug("Observing pool size={}", this.modelPool.capacity());
  }

  /**
   * Sets the flag to terminate this instance an commit all remaining models.
   */
  public void terminate() {
    this.terminate = true;
  }

  /**
   * Checks if a model has changed data and if it has commits it to the data
   * provider.
   *
   * @param modelEntry Document-id, Document-Model pair
   */
  private boolean commitModel(final Entry<Integer, DocumentModel> modelEntry) {
    boolean removed = false;
    // try to lock model
    if (this.lockedModels.add(modelEntry.getKey())) {
      // ok, model is not already locked
      if (this.modelPool.remove(modelEntry.getKey()) != null) {
        // now model has been removed from queue
        if (modelEntry.getValue().hasChanged()) {
          // model has changed data, finalize it
          modelEntry.getValue().setChanged(false);
          // lock model to prevent changes
          modelEntry.getValue().lock();
          // commit model
          this.dataProv.updateDocumentModel(modelEntry.getValue());
        }
        // no changed data in model, or already commited, remove model
        this.lockedModels.remove(modelEntry.getKey());
        removed = true;
      }
      this.lockedModels.remove(modelEntry.getKey());
    }
    return removed;
  }

  @Override
  public void run() {
    final TimeMeasure tm = new TimeMeasure().start();
    while (!terminate) {
      if (tm.getElapsedSeconds() > 15) {
        LOG.info("Pool size {}", this.modelPool.size());
        tm.start();
      }

      // commit entries, if pool is ~2/3 filled
      if (!this.modelPool.isEmpty() && this.modelPool.size() > (this.modelPool.
              capacity() * POOL_LOAD)) {
        for (Entry<Integer, DocumentModel> modelEntry : this.modelPool.
                entrySet()) {
          if (commitModel(modelEntry)) {
            break;
          }
        }
      }
    }
    LOG.info("Pool observer terminating. Commiting all pending models.");
    while (!this.modelPool.isEmpty()) {
      for (Entry<Integer, DocumentModel> modelEntry : this.modelPool.entrySet()) {
        if (!commitModel(modelEntry) && LOG.isDebugEnabled()) {
          LOG.debug("Failed to commit docId={}. Retrying in next loop.",
                  modelEntry.getKey());
        }
      }
    }
  }
}
