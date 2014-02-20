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
package de.unihildesheim.util.concurrent.processing;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps the given {@link Collection} as {@link Source}. Thread safety is
 * handled by using a synchronized collection wrapper. If the collection gets
 * modified while being used as source, the behavior is undefined.
 *
 * @param <T> Type of the collections elements
 */
public final class CollectionSource<T>
        extends Source<T> implements ObservableSource {

  /**
   * Wrapped collection acting as source.
   */
  private final Collection<T> collection;
  /**
   * Iterator over the wrapped source.
   */
  private volatile Iterator<T> itemsIt;
  /**
   * Number of provided items.
   */
  private final AtomicInteger sourcedItemCount;

  /**
   * Wrap the specified collection using it as source.
   *
   * @param coll Collection to wrap
   */
  public CollectionSource(final Collection<T> coll) {
    super();
    this.sourcedItemCount = new AtomicInteger(0);
    this.collection = coll;
  }

  @Override
  public synchronized T next() throws ProcessingException,
          InterruptedException {
    if (itemsIt.hasNext()) {
      this.sourcedItemCount.incrementAndGet();
      return itemsIt.next();
    }
    stop();
    return null;
  }

  @Override
  public Integer getItemCount() throws ProcessingException {
    checkRunStatus();
    return this.collection.size();
  }

  @Override
  public synchronized void run() {
    if (isRunning()) {
      throw new ProcessingException.SourceIsRunningException();
    }
    this.itemsIt = this.collection.iterator();
    super.run();
  }

  @Override
  public int getSourcedItemCount() {
    return this.sourcedItemCount.get();
  }

}
