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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps the given {@link Iterator} as {@link Source}.
 *
 * @param <T> Type of elements
 * @author Jens Bertram
 */
public class IteratorSource<T>
    extends Source<T> {
  /**
   * Items source.
   */
  private final Iterator<T> itemsIt;
  /**
   * Number of provided items.
   */
  private final AtomicLong sourcedItemCount;

  public IteratorSource(final Iterator<T> newItemsIt) {
    this.itemsIt = newItemsIt;
    this.sourcedItemCount = new AtomicLong(0L);
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

  @SuppressWarnings("ReturnOfNull")
  @Override
  public Long getItemCount()
      throws ProcessingException {
    // no item count information available
    return null;
  }
}
