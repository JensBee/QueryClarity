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

import de.unihildesheim.lucene.document.model.DocumentModelPool;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.index.IndexReader;

/**
 * A general source for the data processing pipeline.
 * <p>
 * Base interface for all <tt>DocumentModel</tt> processors. Basically
 * allowing access to the underlying {@link IndexDataprovider}, the caching
 * {@link DocumentModelPool}, the Lucene index and thread tracking functions.
 * <p>
 * A processing pipeline consists of a {@link ProcessingSource}, a
 * {@link ProcessingTarget} and multiple {@link ProcessingWorker}s.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class ProcessingSource {

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "ProcessingSource_";

  /**
   * Provider for statistical index related informations.
   */
  private final IndexDataProvider dataProvider;
  /**
   * Unbound cached for <tt>DocumentModels</tt> that have been processed.
   */
  private final DocumentModelPool documentModelPool;
  /**
   * Latch that tracks the running threads.
   */
  private final CountDownLatch threadTrackingLatch;
  /**
   * Number of threads to run.
   */
  private static final int THREADS = ClarityScoreConfiguration.INSTANCE.
          getInt(CONF_PREFIX + "threads", Runtime.getRuntime().
                  availableProcessors());

  /**
   * Create a new source for the processing pipeline.
   *
   * @param indexDataProvider Provider for base data
   */
  public ProcessingSource(final IndexDataProvider indexDataProvider) {
    this.dataProvider = indexDataProvider;
    this.documentModelPool = new DocumentModelPool(this.dataProvider);
    this.threadTrackingLatch = new CountDownLatch(THREADS);
  }

  /**
   * Get the {@link IndexDataProvider} used by this instance.
   *
   * @return Data provider used by this instance
   */
  public final IndexDataProvider getDataProvider() {
    return this.dataProvider;
  }

  /**
   * Get the {@link DocumentModelPool} held by this instance.
   *
   * @return Document model pool
   */
  public final DocumentModelPool getDocumentModelPool() {
    return this.documentModelPool;
  }

  /**
   * Get the reader used to access the Lucene index.
   *
   * @return Reader pointing at Lucenes index
   */
  public abstract IndexReader getIndexReader();

  /**
   * Get the number of threads used.
   *
   * @return Number of threads
   */
  public final int getThreadCount() {
    return THREADS;
  }

  /**
   * Get the latch for tracking the running threads.
   *
   * @return Thread tracking latch
   */
  public final CountDownLatch getTrackingLatch() {
    return this.threadTrackingLatch;
  }

  /**
   * Base class for {@link ProcessingSource}s using a {@link BlockingQueue}
   * for managing items to process.
   *
   * @param <T> Type of items that get queued
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected abstract static class ProcessingQueueSource<T> extends
          ProcessingSource {

    /**
     * Queued items to process.
     */
    private final BlockingQueue<T> queue;

    /**
     * Create a queue processing source with a {@link IndexDataProvider} to
     * get index related data and a maximum capacity for the working queue.
     *
     * @param indexDataProvider Provider for index data
     * @param queueMaxCap Maximum number of queued work items
     */
    public ProcessingQueueSource(final IndexDataProvider indexDataProvider,
            final int queueMaxCap) {
      super(indexDataProvider);
      this.queue = new ArrayBlockingQueue<>(queueMaxCap);
    }

    /**
     * Get the queue of document-ids that needs to be processed.
     *
     * @return Terms queue
     */
    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    public final BlockingQueue<T> getQueue() {
      return this.queue;
    }

    /**
     * Get the number of items that should be processed by the queue.
     *
     * @return Number of terms to process
     */
    public abstract long getNumberOfItemsToProcess();

    /**
     * Get an iterator over all terms that should be put onto the queue.
     *
     * @return Iterator over all terms to be processed
     */
    public abstract Iterator<T> getItemsToProcessIterator();
  }

  /**
   * Specific {@link ProcessingSource} creating a queue of document-ids to
   * process.
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class DocQueue extends ProcessingQueueSource<Integer> {

    /**
     * Maximum number of document-ids that should be queued.
     */
    private static final int THREAD_QUEUE_MAX_CAPACITY
            = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                    + "docQueue_threadQueueMaxCap", THREADS * 20);

    /**
     * Create a new document-id source, backed by a {@link IndexDataProvider}.
     *
     * @param indexDataProvider Data provider
     */
    public DocQueue(final IndexDataProvider indexDataProvider) {
      super(indexDataProvider, THREAD_QUEUE_MAX_CAPACITY);
    }
  }

  /**
   * Specific {@link ProcessingSource} creating a queue of terms to process.
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class TermQueue extends
          ProcessingQueueSource<BytesWrap> {

    /**
     * Maximum number of terms that should be queued.
     */
    private static final int THREAD_QUEUE_MAX_CAPACITY
            = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                    + "termQueue_threadQueueMaxCap", THREADS * 20);

    /**
     * Create a new term source, backed by a {@link IndexDataProvider}.
     *
     * @param indexDataProvider Data provider
     */
    public TermQueue(final IndexDataProvider indexDataProvider) {
      super(indexDataProvider, THREAD_QUEUE_MAX_CAPACITY);
    }
  }
}
