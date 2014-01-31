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
import de.unihildesheim.lucene.scoring.clarity.DefaultClarityScorePrecalculator;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

/**
 * Factory for spawning {@link DocumentModelUpdater} threads.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelUpdaterFactory implements ThreadFactory {

  /**
   * Counts number of spawned threads.
   */
  private int threadCounter = 0;
  /**
   * {@link ClarityScorePrecalculator} instance using this factory.
   */
  private final ClarityScorePrecalculator pCalcInstance;

  /**
   * Create a new factory for a given {@link ClarityScorePrecalculator}
   * instance.
   *
   * @param parentInstance {@link ClarityScorePrecalculator} instance using this
   * factory
   */
  public DocumentModelUpdaterFactory(
          final ClarityScorePrecalculator parentInstance) {
    this.pCalcInstance = parentInstance;
  }

  /**
   * Factory method to generate a new {@link DocumentModelUpdater} instance. The
   * specific {@link ClarityScorePrecalculator} type is guessed based on the
   * instance of the <tt>parentInstance</tt> parameter.
   *
   * @param docId Document id to process
   * @param terms Terms to process
   * @return A new {@link DocumentModelUpdater} instance matching the given
   * {@link ClarityScorePrecalculator} instance
   */
  public DocumentModelUpdater newInstance(final Integer docId,
          final BytesWrap[] terms) {
    if (pCalcInstance instanceof DefaultClarityScorePrecalculator) {
      return new DefaultClarityScorePrecalculator.DCSDocumentModelUpdater(
              (DefaultClarityScorePrecalculator) pCalcInstance, docId, terms);
    }
    throw new IllegalArgumentException("Unknown instance '" + pCalcInstance.
            getClass() + "'");
  }

  @Override
  public Thread newThread(Runnable r) {
    return new Thread(r, "DMUpdater-" + (++threadCounter));
  }
}
