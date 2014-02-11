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
package de.unihildesheim.lucene.document.model.processing;

import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.document.model.DocumentModelPool;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import org.slf4j.LoggerFactory;

/**
 * {@link ProcessingWorker.DocTerms} processing worker. Gets instantiated by a
 * {@link ThreadFactory} and works on a single document processing a list of
 * terms.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class WorkerDocumentTerm implements ProcessingWorker.DocTerms {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          WorkerDocumentTerm.class);

  /**
   * Id of the document this updater works on.
   */
  private final Integer docId;
  /**
   * List of terms to check.
   */
  private final BytesWrap[] terms;
  /**
   * Name of this thread.
   */
  private String tName;
  /**
   * Shared pool of cached document models.
   */
  private final DocumentModelPool pool;

  /**
   * Create a new updater working thread.
   *
   * @param docModelPool Shared pool of cached document models
   * @param currentDocId Document-id to update
   * @param newTerms Terms to use for updating
   */
  public WorkerDocumentTerm(
          final DocumentModelPool docModelPool,
          final Integer currentDocId,
          final BytesWrap[] newTerms) {
    if (currentDocId == null) {
      throw new IllegalArgumentException("Document-id was null.");
    }
    if (docModelPool == null) {
      throw new IllegalArgumentException("Document-pool was null.");
    }
    if (newTerms == null) {
      throw new IllegalArgumentException("Terms were null.");
    }
    this.docId = currentDocId;
    this.pool = docModelPool;
    this.terms = new BytesWrap[newTerms.length];
    // make a local copy of the passed in terms
    int idx = 0;
    for (BytesWrap term : newTerms) {
      if (term != null) {
        this.terms[idx++] = term.clone();
      }
    }
  }

  /**
   * Method to actually calculate the value for the document model.
   *
   * @param docModel Document model to calculate
   * @param term Term to do the calculation for
   */
  protected abstract void doWork(final DocumentModel docModel,
          final BytesWrap term);

  @Override
  public final void terminate() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public final void run() {
    this.tName = Thread.currentThread().getName();
    LOG.trace("({}) Updating {}", this.tName, this.docId);
    // wrapped in try block, to ensure model gets unlocked and the latch
    // gets decreased
    try {
      // document is already locked for this updater
      DocumentModel docModel = this.pool.get(this.docId);
      if (docModel == null) {
        LOG.warn("({}) Error retrieving document with id={}. Got null.",
                this.tName, this.docId);
        LOG.trace("({}) Finished updating {}", this.tName, this.docId);
        return;
      }

      // go through all query terms..
      for (BytesWrap term : this.terms) {
        // .. and check if the current document matched it
        if (docModel.contains(term)) {
          try {
            this.doWork(docModel, term);
          } catch (NullPointerException ex) {
            LOG.error("({}) NPE docModel={} docId={} term={}", this.tName,
                    docModel, this.docId, BytesWrapUtil.bytesWrapToString(
                            term), ex);
          }
        }
      }
      LOG.trace("({}) Finished updating {}", this.tName, this.docId);
    } catch (Exception ex) {
      LOG.error("({}) Updater caught an exception.", this.tName, ex);
    } finally {
      // make sure we unlock the model
      this.pool.unLock(docId);
    }
  }
}
