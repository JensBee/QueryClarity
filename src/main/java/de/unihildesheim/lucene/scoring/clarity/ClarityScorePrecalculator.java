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

import de.unihildesheim.lucene.document.DocumentModelPool;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.index.IndexReader;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface ClarityScorePrecalculator {
  /**
   * Get the {@link IndexDataProvider} used by this instance.
   * @return Data provider used by this instance
   */
  IndexDataProvider getDataProvider();

  /**
   * Get the {@link DocumentModelPool} held by this instance.
   * @return Document model pool
   */
  DocumentModelPool getDocumentModelPool();

  /**
   * Get the shared set of locked models.
   * @return Set of locked models
   */
  Set<Integer> getLockedModelsSet();

  /**
   * Get the reader used to access the Lucene index.
   * @return Reader pointing at Lucenes index
   */
  IndexReader getIndexReader();

  /**
   * Get the queue of terms that needs to be processed.
   * @return Terms queue
   */
  BlockingQueue<BytesWrap> getTermQueue();

  /**
   * Get the latch for tracking the running threads.
   * @return Thread tracking latch
   */
  CountDownLatch getTrackingLatch();
}
