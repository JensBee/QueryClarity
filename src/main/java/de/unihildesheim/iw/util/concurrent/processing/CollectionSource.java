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
package de.unihildesheim.iw.util.concurrent.processing;

import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps the given {@link Collection} as {@link Source}. Thread safety is
 * handled by using a synchronized collection wrapper. If the collection gets
 * modified while being used as source, the behavior is undefined.
 *
 * @param <T> Type of the collections elements
 */
public final class CollectionSource<T>
    extends Source<T> {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      CollectionSource.class);
  /**
   * Wrapped collection acting as source.
   */
  private final Collection<T> collection;
  /**
   * Number of provided items.
   */
  private final AtomicLong sourcedItemCount;
  /**
   * Iterator over the wrapped source.
   */
  private volatile Iterator<T> itemsIt;

  /**
   * Wrap the specified collection using it as source.
   *
   * @param coll Collection to wrap
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public CollectionSource(final Collection<T> coll) {
    if (Objects.requireNonNull(coll, "Collection was null.").isEmpty()) {
      throw new IllegalArgumentException("Empty collection.");
    }
    this.sourcedItemCount = new AtomicLong(0L);
    this.collection = coll;
  }

  @Override
  public synchronized Long call() {
    if (isRunning()) {
      throw new SourceException.SourceIsRunningException();
    }
    this.itemsIt = this.collection.iterator();
    super.call();
    return this.sourcedItemCount.get();
  }

  @Override
  public long getSourcedItemCount() {
    return this.sourcedItemCount.get();
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public synchronized T next() {
    if (isFinished()) {
      throw new SourceException.SourceHasFinishedException();
    }
    if (this.itemsIt != null && this.itemsIt.hasNext()) {
      this.sourcedItemCount.incrementAndGet();
      return this.itemsIt.next();
    }
    stop();
    return null;
  }

  @Override
  public Long getItemCount()
      throws ProcessingException {
    checkRunStatus();
    return (long) this.collection.size();
  }

}
