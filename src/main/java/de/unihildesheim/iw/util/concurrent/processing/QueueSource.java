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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Uses a {@link Queue} as source. Retrieving elements is synchronized to
 * allow multiple threads polling entries.
 * @author Jens Bertram
 */
public class QueueSource<T>
    extends Source<T> {
  /**
   * Number of provided items.
   */
  private final AtomicLong sourcedItemCount;
  /**
   * Source {@link Queue}.
   */
  private final Queue<T> sourceQueue;

  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public QueueSource(final Queue source) {
    this.sourceQueue = source;
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
    if (this.sourceQueue.isEmpty()) {
      stop();
      return null;
    }
    this.sourcedItemCount.incrementAndGet();
    return (T) this.sourceQueue.poll();
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public Long getItemCount()
      throws ProcessingException {
    try {
      return (long) this.sourceQueue.size();
    } catch (final UnsupportedOperationException e) {
      return null;
    }
  }
}
