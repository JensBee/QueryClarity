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
package de.unihildesheim.iw;

import de.unihildesheim.iw.util.RandomValue;

/**
 * Test methods for {@link SupportsPersistence} interface.
 *
 * @author Jens Bertram
 */
public final class SupportsPersistenceTestMethods {

  private SupportsPersistenceTestMethods() {
  }

  /**
   * Test of loadCache method, of class SupportsPersistence.
   *
   * @param impl Instance implementing {@link SupportsPersistence}
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public static void testLoadCache(final SupportsPersistence impl)
      throws
      Exception {
    impl.loadCache(RandomValue.getString(10));
  }

  /**
   * Test of loadOrCreateCache method, of class SupportsPersistence.
   *
   * @param impl Instance implementing {@link SupportsPersistence}
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public static void testLoadOrCreateCache(final SupportsPersistence impl)
      throws Exception {
    impl.loadOrCreateCache(RandomValue.getString(10));
  }

  /**
   * Test of createCache method, of class SupportsPersistence.
   *
   * @param impl Instance implementing {@link SupportsPersistence}
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public static void testCreateCache(final SupportsPersistence impl)
      throws
      Exception {
    impl.createCache(RandomValue.getString(10));
  }
}
