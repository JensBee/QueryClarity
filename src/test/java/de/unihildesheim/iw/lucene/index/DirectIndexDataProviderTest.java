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
package de.unihildesheim.iw.lucene.index;

/**
 * Test for {@link DirectIndexDataProvider}.
 * <p>
 * Some test methods are kept here (being empty) to satisfy Netbeans automatic
 * test creation routine.
 *
 * @author Jens Bertram
 */
@SuppressWarnings({"UnusedDeclaration", "EmptyMethod"})
public final class DirectIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * {@link IndexDataProvider} class being tested.
   */
  private static final Class<? extends IndexDataProvider> DATAPROV_CLASS
      = DirectIndexDataProvider.class;

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public DirectIndexDataProviderTest()
      throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL),
        DATAPROV_CLASS);
  }

  /**
   * Test of createCache method, of class DirectIndexDataProvider.
   *
   * @see #testCreateCache__plain()
   */
  public void testCreateCache() {
    // implemented in super class
  }

  /**
   * Test of loadOrCreateCache method, of class DirectIndexDataProvider.
   *
   * @see #testLoadOrCreateCache__plain()
   */
  public void testLoadOrCreateCache() {
    // implemented in super class
  }

  /**
   * Test of loadCache method, of class DirectIndexDataProvider.
   *
   * @see #testLoadCache__plain()
   */
  public void testLoadCache() {
    // implemented in super class
  }

  /**
   * Test of warmUp method, of class DirectIndexDataProvider.
   *
   * @see #testWarmUp__plain()
   */
  public void testWarmUp() {
    // implemented in super class
  }

  /**
   * Test of warmUpDocumentFrequencies method, of class DirectIndexDataProvider.
   *
   * @see #testWarmUpDocumentFrequencies__plain()
   */
  public void testWarmUpDocumentFrequencies() {
    // implemented in super class
  }

  /**
   * Test of getDocumentIds method, of class DirectIndexDataProvider.
   *
   * @see #testGetDocumentIds__plain()
   */
  public void testGetDocumentIds() {
    // implemented by super class
  }

  /**
   * Test of getDocumentModel method, of class DirectIndexDataProvider.
   *
   * @see #testGetDocumentModel__plain()
   */
  public void testGetDocumentModel() {
    // implemented in super class
  }

  /**
   * Test of getDocumentsTermSet method, of class DirectIndexDataProvider.
   *
   * @see #testGetDocumentsTermSet__plain()
   */
  public void testGetDocumentsTermSet() {
    // implemented in super class
  }

  /**
   * Test of documentContains method, of class DirectIndexDataProvider.
   *
   * @see #testDocumentContains__plain()
   */
  public void testDocumentContains() {
    // implemented in super class
  }

  /**
   * Test of getDocumentFrequency method, of class DirectIndexDataProvider.
   *
   * @see #testGetDocumentFrequency__plain()
   */
  public void testGetDocumentFrequency() {
    // implemented in super class
  }
}
