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
package de.unihildesheim.util;

/**
 * Interface for objects allowing to switch their data-model between being
 * mutable and immutable.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public interface Lockable {

  /**
   * Lock the instance, rendering it's data immutable.
   */
  void lock();

  /**
   * Un-lock the instance to make it mutable again.
   */
  void unlock();

  /**
   * Get the locked state of this instance.
   *
   * @return True if instance is locked.
   */
  boolean isLocked();
}
