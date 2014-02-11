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

import de.unihildesheim.lucene.util.BytesWrap;
import java.util.concurrent.ThreadFactory;

/**
 * A general worker thread for the data processing pipeline, working on results
 * from a {@link ProcessingTarget}.
 * <p>
 * A processing pipeline consists of a {@link ProcessingSource}, a
 * {@link ProcessingTarget} and multiple {@link ProcessingWorker}s.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface ProcessingWorker extends Runnable {

  /**
   * Sets the termination flag for the current thread.
   */
  void terminate();

  /**
   * Factory to create new {@link ProcessingWorker} instances processing an
   * unspecified type.
   */
  interface Factory extends ThreadFactory {
  }

  /**
   * Specific {@link ProcessingWorker} instance processing a document and a list
   * of terms.
   */
  interface DocTerms extends ProcessingWorker {

    /**
     * Factory to create new {@link ProcessingWorker} instances processing a
     * document and a list of terms.
     */
    interface Factory extends ProcessingWorker.Factory {

      /**
       * Create a new {@link ProcessingWorker} instance processing a document
       * and a list of terms.
       *
       * @param docId Document-id to process
       * @param terms Terms to use for processing
       * @return New worker instance
       */
      ProcessingWorker.DocTerms newInstance(final Integer docId,
              final BytesWrap[] terms);
    }
  }
}
