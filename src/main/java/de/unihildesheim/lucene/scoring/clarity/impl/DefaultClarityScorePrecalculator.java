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
package de.unihildesheim.lucene.scoring.clarity.impl;

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Threaded document model pre-calculation. This calculation may be very
 * expensive in time, so this class makes heavy use of parallel calculations
 * to try to minimize th needed time.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScorePrecalculator {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScorePrecalculator.class);

  /**
   * Parent calculation instance.
   */
  private final DefaultClarityScore dcsInstance;

  /**
   * Creates a new document-model pre-calculator for the given
   * {@link DefaultClarityScore} instance.
   *
   * @param dcs Instance to run the calculation for
   */
  public DefaultClarityScorePrecalculator(final DefaultClarityScore dcs) {
    this.dcsInstance = dcs;
  }

  /**
   * Get the {@link DefaultClarityScore} instance using this calculator.
   *
   * @return {@link DefaultClarityScore} instance using this calculator
   */
  protected DefaultClarityScore getDcsInstance() {
    return dcsInstance;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   */
  public void preCalculate() {
    LOG.info("Pre-calculating {} models.", CollectionMetrics.
            numberOfDocuments());
    final Processing pPipe = new Processing(new DocumentModelCalculator(
            Environment.getDataProvider().getDocumentIdSource()));
    pPipe.process();
  }

  /**
   * {@link Processing.Target} creating document models.
   */
  private final class DocumentModelCalculator
          extends Target<Integer> {

    /**
     * @param source {@link Source} for this {@link Target}
     */
    DocumentModelCalculator(final Source<Integer> source) {
      super(source);
    }

    @Override
    public Target<Integer> newInstance() {
      return new DocumentModelCalculator(getSource());
    }

    @Override
    public void runProcess() throws IOException, ProcessingException,
            InterruptedException {

      while (!isTerminating()) {
        Integer docId;
        try {
          docId = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        if (docId == null) {
          continue;
        }

        DocumentModel docModel = Environment.getDataProvider()
                .getDocumentModel(docId);
        if (docModel == null) {
          LOG.warn("({}) Model for document-id {} was null.", this.
                  getName(), docId);
        } else {
          // call the calculation method of the main class for each
          // document and term that is available for processing
          getDcsInstance().calcDocumentModel(docModel);
        }
      }
    }
  }
}
