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
import de.unihildesheim.lucene.scoring.clarity.ClarityScorePrecalculator;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.TimeMeasure;
import java.util.Set;
import org.slf4j.LoggerFactory;

/**
 * Updater thread instance for threaded document model pre-calculation. Used by
 * {@link DocumentModelCalulator}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class DocumentModelUpdater implements Runnable {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DocumentModelUpdater.class);
  /**
   * {@link ClarityScorePrecalculator} instance using this updater.
   */
  private final ClarityScorePrecalculator cspInstance;
  /**
   * Id of the document this updater works on.
   */
  protected final Integer docId;
  /**
   * List of terms to check.
   */
  protected final BytesWrap[] queryTerms;
  /**
   * Name of this thread.
   */
  protected String tName;

  public DocumentModelUpdater(final ClarityScorePrecalculator pCalcInstance,
          final Integer currentDocId,
          final BytesWrap[] terms) {
    this.docId = currentDocId;
    this.queryTerms = terms;
    this.cspInstance = pCalcInstance;
  }

  /**
   * Method to actually calculate the value for the document model.
   *
   * @param docModel Document model to calculate
   * @param term Term to do the calculation for
   */
  protected abstract void calculate(final DocumentModel docModel,
          final BytesWrap term);

  @Override
  public final void run() {
    this.tName = Thread.currentThread().getName();
    LOG.trace("({}) Updating {}", this.tName, this.docId);
    // wrapped in try block, to ensure model gets unlocked and the latch
    // gets decreased
    try { // FIXME: replace try catch block
      DocumentModel docModel = this.cspInstance.getDocumentModelPool().get(
              this.docId);
      if (docModel == null) {
        docModel = this.cspInstance.getDataProvider().getDocumentModel(
                this.docId);
        if (docModel == null) {
          LOG.warn("({}) Error retrieving document with id={}. Got null.",
                  this.tName, this.docId);
          // error - release model and continue with next
          this.cspInstance.getLockedModelsSet().remove(this.docId);
          LOG.trace("({}) Finished updating {}", this.tName, this.docId);
          return;
        }
      }
      this.cspInstance.getDocumentModelPool().put(docModel);

      // go through all query terms..
      final TimeMeasure tm = new TimeMeasure().start();
      for (BytesWrap term : queryTerms) {
        // .. and check if the current document matched it
        if (docModel.containsTerm(term)) {
          try {
            this.calculate(docModel, term);
          } catch (NullPointerException ex) {
            LOG.error("({}) NPE docModel={} docId={} term={}", this.tName,
                    docModel, this.docId, BytesWrapUtil.bytesWrapToString(
                            term), ex);
          }
        }
      }
      LOG.trace("({}) Finished updating {}", this.tName, this.docId);
      tm.stop();
      if (((int) tm.getElapsedSeconds()) > 0) {
        LOG.debug("({}) Updating model for {} terms took {}.", this.tName,
                queryTerms.length, tm.getElapsedTimeString());
      }
    } catch (Exception ex) {
      LOG.error("({}) Updater caught an exception.", this.tName, ex);
    } finally {
      this.cspInstance.getLockedModelsSet().remove(this.docId);
    }
  }
}
