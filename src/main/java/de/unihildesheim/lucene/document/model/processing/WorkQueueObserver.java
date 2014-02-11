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
import de.unihildesheim.util.Tuple;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class WorkQueueObserver {

  private WorkQueueObserver() {

  }

  private static abstract class AbtractWorkQueueObserver<T> implements Runnable {

    /**
     * Name for this runnable.
     */
    protected String tName;
    /**
     * Termination flag.
     */
    private boolean terminate = false;
    /**
     * Randomizer.
     */
    private Random rnd = new Random();
    /**
     * Logger instance for this class.
     */
    private org.slf4j.Logger log;

    /**
     * Processing source.
     */
    private final ProcessingSource source;
    /**
     * Factory creating worker threads.
     */
    private final ProcessingWorker.Factory workerFactory;
    /**
     * Queue for items that should be processed.
     */
    private final BlockingDeque<T> workQueue;

    abstract boolean tryProcessItem(final T item);

    AbtractWorkQueueObserver(final String name,
            final ProcessingSource newSource,
            final ProcessingWorker.Factory newWorkerFactory,
            final BlockingDeque<T> sharedWorkQueue) {
      this.source = newSource;
      this.workerFactory = newWorkerFactory;
      this.workQueue = sharedWorkQueue;
      this.tName = "WorkQueueObserver(" + name + "_" + this.hashCode() + ")";
    }

    public void terminate() {
      log.debug("({}) " + DocTermWorkQueueObserver.class
              + " got terminating signal.", this.tName);
      this.terminate = true;
    }

    @Override
    public void run() {
      this.log = LoggerFactory.
              getLogger(TargetDocTerms.class + "_" + this.tName);
      log.debug("({}) Starting", tName);
      while (!this.terminate || !(this.terminate
              && this.workQueue.isEmpty())) {
        // pre-check, if we have items
        if (this.workQueue.isEmpty()) {
          continue;
        }

        T item;
        // peek at the head of queue first
        item = this.workQueue.peekFirst();
        if (!tryProcessItem(item)) {
          // peek at the tail of queue seconds, if needed
          item = this.workQueue.peekLast();

          if (!tryProcessItem(item)) {
            Iterator<T> itemIt;

            // pick a random iteration direction
            if (rnd.nextBoolean()) {
              itemIt = this.workQueue.iterator();
            } else {
              itemIt = this.workQueue.descendingIterator();
            }

            while (itemIt.hasNext()) {
              if (tryProcessItem(itemIt.next())) {
                break;
              }
            }
          }
        }
      }
    }
  }

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          TargetDocTerms.class);

  /**
   * Thread to observe the queue of document-terms pairs to process. Each thread
   * will poll from the queue, check if the model is not locked and run the
   * desired worker for each entry.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class DocTermWorkQueueObserver extends
          AbtractWorkQueueObserver<Tuple.Tuple2<Integer, BytesWrap[]>> {

    /**
     * Document-ids source.
     */
    private final ProcessingSource source;
    /**
     * Factory creating worker threads.
     */
    private final ProcessingWorker.DocTerms.Factory workerFactory;
    /**
     * Queue for documents that should be processed.
     */
    private final BlockingDeque<Tuple.Tuple2<Integer, BytesWrap[]>> workQueue;

    public DocTermWorkQueueObserver(final String name,
            final ProcessingSource docSource,
            final ProcessingWorker.DocTerms.Factory termsTargetFactory,
            final BlockingDeque<Tuple.Tuple2<Integer, BytesWrap[]>> sharedWorkQueue) {
      super(name, docSource, termsTargetFactory, sharedWorkQueue);
      this.source = docSource;
      this.workerFactory = termsTargetFactory;
      this.workQueue = sharedWorkQueue;
    }

    /**
     * Spawns the worker process for a document and term list.
     *
     * @param item Tuple describing document and terms used for processing
     * @return True, if item was processed successfully (runnable was executed)
     */
    @Override
    boolean tryProcessItem(final Tuple.Tuple2<Integer, BytesWrap[]> item) {
      boolean processed;
      // try to lock model
      if (this.source.getDocumentModelPool().lock(item.a)) {
        // locked - still in queue?
        if (this.workQueue.remove(item)) {
          // locked & in queue - process it
          try {
            this.workerFactory.newInstance(item.a, item.b).run();
            processed = true;
          } catch (Exception ex) {
            LOG.error("({}) WorkQueueObserver caught exception "
                    + "while executing worker.", this.tName, ex);
            processed = false;
          }
        } else {
          processed = false;
        }
        // unlock - we're done
        this.source.getDocumentModelPool().unLock(item.a);
      } else {
        processed = false;
      }
      return processed;
    }
  }
}
