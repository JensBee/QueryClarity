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

import java.util.Collection;

/**
 * Wraps the given {@link Collection} as {@link Source}.
 *
 * @param <T> Type of the collections elements
 * @author Jens Bertram
 */
public final class CollectionSource<T>
    extends IterableSource<T> {
  /**
   * Number of items in the source collection.
   */
  private final long itemsCount;

  /**
   * Wrap the specified collection using it as source.
   *
   * @param coll Collection to wrap
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  public CollectionSource(final Collection<T> coll) {
    super(coll);
    this.itemsCount = getItemsCount(coll);
  }

  /**
   * Get the number of items present in the source collection. Will manually
   * count the number of entries, if it's larger than {@link
   * Integer#MAX_VALUE}.
   *
   * @param coll Collection whose items to count
   * @return Number of items in the collection
   */
  private static long getItemsCount(final Collection coll) {
    final int count = coll.size();
    // worst case - we have to calculate the real size
    if (count == Integer.MAX_VALUE) {
      long manualCount = 0L;
      for (final Object ignored : coll) {
        manualCount++;
      }
      return manualCount;
    }
    return (long) count;
  }

  @Override
  public Long getItemCount() {
    return this.itemsCount;
  }

}
