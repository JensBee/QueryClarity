/*
 * Copyright (C) 2014 Jens Bertram
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
package de.unihildesheim.iw.lucene.index;

/**
 * @author Jens Bertram
 */
public interface SupportsPersistence {

  /**
   * Load a persistent cache with the given name.
   *
   * @param name Name of the cache to create
   * @throws Exception Thrown, if cache loading fails
   */
  void loadCache(final String name)
      throws Exception;

  /**
   * Tries to load a persistent cache with the given name. If none is found a
   * new one with the given name is created.
   *
   * @param name Name of the cache to create
   * @throws Exception Thrown, if cache loading/creation fails
   */
  void loadOrCreateCache(final String name)
      throws Exception;

  /**
   * Creates a new persistent cache with the given name.
   *
   * @param name Name of the cache to create
   * @throws Exception Thrown, if cache creation fails
   */
  void createCache(final String name)
      throws Exception;
}
