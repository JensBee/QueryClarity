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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes the pool of cached {@link DocumentModel}s to dequeue and update the
 * data store as needed.
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelPoolObserver implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final transient Logger LOG = LoggerFactory.getLogger(
          DocumentModelPoolObserver.class);

  /**
   * Factor defining the limit after which entries get commited to the
   * {@link IndexDataProvider}.
   */
  private static final double LOAD_FACTOR = 0.75;
  /**
   * Calculated value of how much free capacity should remain in the pool.
   */
  private final int remainingCount;
  /**
   * Pool to observe.
   */
  private final DocumentModelPool modelQueue;
  /**
   * Flag indicating if this instance should terminate.
   */
  private boolean terminate = false;
  /**
   * {@link IndexDataProvider} to commit changes to.
   */
  private final IndexDataProvider dataProv;

  /**
   * Creates a new observer for the given pool and queue.
   *
   * @param newDataProv Data provider to commit to
   * @param newModelQueue Queue to observe
   */
  public DocumentModelPoolObserver(final IndexDataProvider newDataProv,
          final DocumentModelPool newModelQueue) {
    this.modelQueue = newModelQueue;
    this.dataProv = newDataProv;
    this.remainingCount = (int) (this.modelQueue.capacity() * LOAD_FACTOR);
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
   * @param docModel Document model to commit
   */
  private void commitModel(final DocumentModel docModel) {
    if (docModel.hasChanged()) {
      docModel.setChanged(false); // finalize it
      this.dataProv.updateDocumentModel(docModel);
    }
  }

  @Override
  public void run() {
    while (!terminate) {
      if (this.modelQueue.remainingCapacity() <= this.remainingCount) {
        try {
          commitModel(this.modelQueue.take());
        } catch (InterruptedException ex) {
          LOG.warn("Writer thread interrupted. Data may be corrupted.");
          this.terminate = true;
        }
      }
    }
    try {
      LOG.debug("Terminate. Commiting all pending models.");
      while (!this.modelQueue.isEmpty()) {
        commitModel(this.modelQueue.take());
      }
    } catch (InterruptedException ex) {
      LOG.warn("Writer thread interrupted. Data may be corrupted.");
    }
  }
}
