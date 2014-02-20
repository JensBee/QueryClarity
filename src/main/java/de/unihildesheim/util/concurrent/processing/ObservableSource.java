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

/**
 * Interface to provide access for {@link Processing.SourceObserver} access.
 */
public interface ObservableSource {

  /**
   * Get the number of items that will be provided.
   *
   * @return Number of items, or <tt>null</tt> if there is no such
   * information.
   * @throws ProcessingException Thrown, if source has not been started, or
   * has already finished
   */
  Integer getItemCount() throws ProcessingException;

  /**
   * Get the number of items already served.
   *
   * @return Number of items served
   */
  int getSourcedItemCount();
}
