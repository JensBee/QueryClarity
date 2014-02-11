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
package de.unihildesheim.lucene.document.model;

import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.util.TimeMeasure;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches document model instances for faster access. This is currently a
 * simple wrapper around a {@link ConcurrentHashMap} instance.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentModelPool {

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "DMPool_";

  /**
   * Unbound list of models being currently modified.
   */
  private final Set<Integer> lockedModels;

  /**
   * Size of the pool.
   */
  private final int size;

  /**
   * Data provider, to get new models from the index, if needed.
   */
  private final IndexDataProvider dataProv;

  /**
   * Default pool size.
   */
  private static final int DEFAULT_POOL_SIZE
          = ClarityScoreConfiguration.INSTANCE.getInt(CONF_PREFIX
                  + "defaultPoolSize", 5000);

  /**
   * Backing map store.
   */
  private Map<Integer, DocumentModel> map;

  /**
   * Creates a new {@link DocumentModel} pool of the given initial size.
   *
   * @param dataProvider Provider for statistical index data
   * @param newSize Size of the pool
   */
  public DocumentModelPool(final IndexDataProvider dataProvider,
          final int newSize) {
    if (dataProvider == null) {
      throw new NullPointerException("IndexDataProvider was null.");
    }
    this.map = new ConcurrentHashMap<>(newSize);
    this.size = newSize;
    this.lockedModels = new ConcurrentSkipListSet<>();
    this.dataProv = dataProvider;
  }

  /**
   * Create a new {@link DocumentModel} pool of default initial size.
   *
   * @param dataProvider Data provider to get new models from
   */
  public DocumentModelPool(final IndexDataProvider dataProvider) {
    this(dataProvider, DEFAULT_POOL_SIZE);
  }

  /**
   * Get the capacity (maximum size) of this pool.
   *
   * @return Size of the pool
   */
  public int capacity() {
    return this.size;
  }

  /**
   * Get the current size of the pool.
   *
   * @return Pool size
   */
  public int size() {
    return this.map.size();
  }

  /**
   * Check if the pool is empty.
   *
   * @return True, if pool is empty
   */
  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  /**
   * Get a set of entries stored in the pool.
   *
   * @return Pool entries
   */
  public Set<Entry<Integer, DocumentModel>> entrySet() {
    return this.map.entrySet();
  }

  /**
   * Removes a specific model from the pool, if it is unlocked. This does not
   * check if the model is currently part of the pool. It only checks if the
   * model is not locked and tries to remove it. If it's already removed
   * nothing will happen (the pool stays unchanged).
   *
   * @param docId Document-id to remove
   * @return True, if model was not locked and removal was triggered, false if
   * it was locked and removal was not tried
   */
  public boolean removeIfUnlocked(final int docId) {
    // temporary lock to remove
    if (this.lockedModels.add(docId)) {
      this.map.remove(docId);
      this.lockedModels.remove(docId);
      return true;
    }
    // add failed - locked
    return false;
  }

  /**
   * Tries to get a {@link DocumentModel} from the current pool state and
   * pulls it into the pool, if its not already there. This does not check if
   * the model is currently locked.
   *
   * @param docId Document-id to lookup
   * @return Document-model for the given document-id or null, if none found
   */
  public DocumentModel get(final int docId) {
    DocumentModel docModel = this.map.get(docId);
    if (docModel == null) {
      docModel = this.dataProv.getDocumentModel(docId);
      if (docModel != null) {
        // add model to cache
        this.map.put(docId, docModel);
      }
    }
    return docModel;
  }

  /**
   * Try to lock a model.
   *
   * @param docId Document id to lock
   * @return True on success, false if it's already locked
   */
  public boolean lock(final int docId) {
    return this.lockedModels.add(docId);
  }

  /**
   * Unlock a previously locked model.
   *
   * @param docId Document-id to unlock
   * @return True if succeeded, false otherwise
   */
  public boolean unLock(final int docId) {
    return this.lockedModels.remove(docId);
  }

  /**
   * Get a unmodifiable view of the locked models set.
   *
   * @return List of locked model ids
   */
  public Set<Integer> getLocks() {
    return Collections.unmodifiableSet(this.lockedModels);
  }

  /**
   * Observes the pool of cached {@link DocumentModel}s to dequeue and update
   * the data store as needed.
   */
  public static final class Observer implements Runnable {

    /**
     * Logger instance for this class.
     */
    private static final transient Logger LOG = LoggerFactory.getLogger(
            Observer.class);

    /**
     * Prefix used to store configuration.
     */
    private static final String CONF_PREFIX = "DMPoolObserver_";
    /**
     * Pool to observe.
     */
    private final DocumentModelPool modelPool;
    /**
     * Flag indicating if this instance should terminate.
     */
    private boolean terminate = false;
    /**
     * {@link IndexDataProvider} to commit changes to.
     */
    private final IndexDataProvider dataProv;
    /**
     * Load factor after which models from the pool should be commited to make
     * room for new entries.
     */
    private static final double POOL_LOAD
            = ClarityScoreConfiguration.INSTANCE.getDouble(CONF_PREFIX
                    + "poolLoad", 0.75);
    /**
     * Maximum size of the pool (held entries) after which to begin commiting
     * models.
     */
    private final int maxPoolSize;
    /**
     * Global shared running threads tracking latch.
     */
    private final CountDownLatch latch;

    /**
     * Flag to indicate if an observer is running. This is to prevent
     * instantiating another one.
     */
    private static boolean isRunning = false;

    /**
     * Creates a new observer for the given pool and queue.
     *
     * @param trackingLatch Global shared running threads tracking latch
     * @param newDataProv Data provider to commit to
     * @param newModelQueue Queue to observe
     */
    private Observer(final CountDownLatch trackingLatch,
            final IndexDataProvider newDataProv,
            final DocumentModelPool newModelQueue) {
      this.modelPool = newModelQueue;
      this.dataProv = newDataProv;
      this.maxPoolSize = (int) (this.modelPool.capacity() * POOL_LOAD);
      this.latch = trackingLatch;
      LOG.info("Pool observer observing pool size={} max={}", this.modelPool.
              capacity(),
              this.maxPoolSize);
    }

    /**
     * Get a new observer instance. There is only one observer allowed. An
     * exception is thrown if you try to create more than one instance.
     *
     * @param trackingLatch Global shared running threads tracking latch
     * @param newDataProv Data provider to commit to
     * @param newModelQueue Queue to observe
     * @return New observer instance
     */
    public static Observer getInstance(final CountDownLatch trackingLatch,
            final IndexDataProvider newDataProv,
            final DocumentModelPool newModelQueue) {
      if (isRunning) {
        throw new IllegalStateException("An observer instance "
                + "is already running.");
      }
      return new Observer(trackingLatch, newDataProv, newModelQueue);
    }

    /**
     * Sets the flag to terminate this instance an commit all remaining
     * models.
     */
    public void terminate() {
      LOG.debug("PoolObserver got terminating signal.");
      this.terminate = true;
    }

    /**
     * Checks if a model has changed data and if it has commits it to the data
     * provider.
     *
     * @param modelEntry Document-id, Document-Model pair
     * @return True, if the model has been removed from the pool
     */
    private boolean commitModel(
            final Entry<Integer, DocumentModel> modelEntry) {
      if (this.modelPool.removeIfUnlocked(modelEntry.getKey())) {
        this.dataProv.updateDocumentModel(modelEntry.getValue());
        return true;
      }
      return false;
    }

    @Override
    public void run() {
      isRunning = true;
      long commitCount = 0;
      final TimeMeasure statusTimeMeasure = new TimeMeasure().start();
      final TimeMeasure overallTimeMeasure = new TimeMeasure().start();
      while (!terminate) {
        if (statusTimeMeasure.getElapsedSeconds() > 15) {
          LOG.info("Pool size {} ({} locks) ({} commits since last status). "
                  + "Observing {}.",
                  this.modelPool.size(), this.modelPool.getLocks().size(),
                  commitCount, overallTimeMeasure.getElapsedTimeString());
          statusTimeMeasure.start();
          commitCount = 0;
        }

        // commit entries, if pool is ~2/3 filled
        if (!this.modelPool.isEmpty() && this.modelPool.size()
                > this.maxPoolSize) {
          for (Entry<Integer, DocumentModel> modelEntry : this.modelPool.
                  entrySet()) {
            if (commitModel(modelEntry)) {
              commitCount++;
              break;
            }
          }
        }
      }
      LOG.info("Pool observer terminating. Commiting all pending models.");
      boolean success;
      while (!this.modelPool.isEmpty()) {
        for (Entry<Integer, DocumentModel> modelEntry : this.modelPool.
                entrySet()) {
          success = commitModel(modelEntry);
          if (LOG.isTraceEnabled()) {
            if (success) {
              LOG.
                      trace("Successfully commited docId={}.", modelEntry.
                              getKey());
            } else {
              LOG.trace("Failed to commit docId={}. Retrying in next loop.",
                      modelEntry.getKey());
            }
          }
        }
      }
      LOG.info("Pool observer terminating after {}. "
              + "All pending models commited.",
              overallTimeMeasure.stop().getElapsedTimeString());
      isRunning = false;
      this.latch.countDown(); // signal we're done
    }
  }
}
