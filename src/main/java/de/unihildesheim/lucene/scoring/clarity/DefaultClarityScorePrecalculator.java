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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.document.model.Processing;
import de.unihildesheim.lucene.document.model.Processing.Source;
import de.unihildesheim.lucene.document.model.Processing.Target;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.util.BytesRef;
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
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "DCSPrecalc_";

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
   * Pre-calculate all document models for all terms known from the index.
   */
  public void preCalculate() {
    final Processing pPipe = new Processing(new DocumentModelCalculator(
            this.dcsInstance.getIndexDataProvider().getDocumentIdSource()));
    pPipe.process();
  }

  public final class DocumentModelCalculator
          extends Processing.Target<Integer> {

    /**
     * Name to identify this {@link Runnable}.
     */
    private final String rId = "(" + DocumentModelCalculator.class + "-"
            + this.hashCode() + ")";
    /**
     * Flag to indicate, if this {@link Runnable} should terminate.
     */
    private volatile boolean terminate;
    /**
     * Shared latch to track running threads.
     */
    private final CountDownLatch latch;

    /**
     * Base constructor without setting a {@link CountDownLatch}. This
     * instance is not able to be run.
     *
     * @param source {@link Source} for this {@link Target}
     */
    DocumentModelCalculator(final Processing.Source<Integer> source) {
      super(source);
      this.terminate = false;
      this.latch = null;
    }

    /**
     * Creates a new instance able to run. Meant to be called from the factory
     * method.
     *
     * @param source @param source {@link Source} for this {@link Target}
     * @param newLatch Shared latch to track running threads
     */
    private DocumentModelCalculator(final Processing.Source<Integer> source,
            final CountDownLatch newLatch) {
      super(source);
      this.terminate = false;
      this.latch = newLatch;
    }

    @Override
    public void terminate() {
      LOG.debug("{} Received termination signal.", this.rId);
      this.terminate = true;
    }

    @Override
    public Processing.Target<Integer> newInstance(
            final CountDownLatch newLatch) {
      return new DocumentModelCalculator(this.getSource(), newLatch);
    }

    @Override
    public void run() {
      final IndexDataProvider dataProv
              = DefaultClarityScorePrecalculator.this.dcsInstance.
              getIndexDataProvider();
      try {
        if (this.latch == null) {
          throw new IllegalStateException(this.rId
                  + " Tracking latch not set.");
        }

        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(
                DefaultClarityScorePrecalculator.this.dcsInstance.getReader(),
                dataProv.getTargetFields());

        while (!this.terminate) {
          if (this.getSource().hasNext()) {
            final Integer docId = this.getSource().next();
            if (docId == null) {
              LOG.warn("{} Document-id was null.", this.rId);
              continue;
            }

            dftEnum.setDocument(docId);
            BytesRef bytesRef = dftEnum.next();
            Collection<BytesWrap> termList = new ArrayList<>();
            while (bytesRef != null) {
              termList.add(new BytesWrap(bytesRef));
              bytesRef = dftEnum.next();
            }

            if (termList.isEmpty()) {
              LOG.warn("{} Empty term list for document-id {}", this.rId,
                      docId);
            } else {
              DocumentModel docModel = dataProv.getDocumentModel(docId);
              if (docModel == null) {
                LOG.warn("{} Model for document-id {} was null.", this.rId,
                        docId);
              } else {
                // call the calculation method of the main class for each
                // document and term that is available for processing
                DefaultClarityScorePrecalculator.this.dcsInstance.
                        calcDocumentModel(docModel, termList);
              }
            }
          }
        }

        LOG.debug("{} Terminating.", this.rId);
      } catch (Exception ex) {
        LOG.debug("{} Caught exception. Terminating.", this.rId, ex);
      } finally {
        this.latch.countDown();
      }
    }
  }
}
