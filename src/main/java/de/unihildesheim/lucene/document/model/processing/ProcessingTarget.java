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

/**
 * A general target for the data processing pipeline, processing data provided
 * by a {@link ProcessingSource}.
 * <p>
 * A processing pipeline consists of a {@link ProcessingSource}, a
 * {@link ProcessingTarget} and multiple {@link ProcessingWorker}s.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface ProcessingTarget extends Runnable {

  /**
   * Sets the termination flag for the current thread.
   */
  void terminate();

  /**
   * Factory to create new {@link ProcessingTarget} instances of unspecified
   * type.
   */
  @SuppressWarnings("PublicInnerClass")
  interface Factory {

    /**
     * Create a new {@link ProcessingTarget} instance of unspecified type.
     *
     * @return New processing instance
     */
    ProcessingTarget newInstance();
  }

  /**
   * Specific {@link ProcessingTarget} instance processing a docQueue.
   */
  @SuppressWarnings("PublicInnerClass")
  interface DocQueue extends ProcessingTarget {

    /**
     * Factory to create new {@link ProcessingTarget} instances processing a
     * docQueue.
     */
    @SuppressWarnings("PublicInnerClass")
    interface Factory extends ProcessingTarget.Factory {

      /**
       * Create a new {@link ProcessingTarget} instance processing a docQueue.
       *
       * @return New processing instance
       */
      @Override
      ProcessingTarget.DocQueue newInstance();
    }
  }

  /**
   * Specific {@link ProcessingTarget} instance processing a termQueue.
   */
  @SuppressWarnings("PublicInnerClass")
  interface TermQueue extends ProcessingTarget {

    /**
     * Factory to create new {@link ProcessingTarget} instances processing a
     * termQueue.
     */
    @SuppressWarnings("PublicInnerClass")
    interface Factory extends ProcessingTarget.Factory {

      /**
       * Create a new {@link ProcessingTarget} instance processing a termQueue.
       *
       * @return New processing instance
       */
      @Override
      ProcessingTarget.TermQueue newInstance();
    }
  }
}
