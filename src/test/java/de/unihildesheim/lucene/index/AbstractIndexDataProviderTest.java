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
package de.unihildesheim.lucene.index;

import de.unihildesheim.ByteArray;
import de.unihildesheim.SerializableByte;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.util.RandomValue;
import java.util.Arrays;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.mapdb.DBMaker;

/**
 * Test for {@link AbstractIndexDataProvider}.
 * <p>
 * Some test methods are kept here (being empty) to satisfy Netbeans automatic
 * test creation routine.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("checkstyle:methodname")
public final class AbstractIndexDataProviderTest
        extends IndexDataProviderTestCase {

  /**
   * {@link IndexDataProvider} class being tested.
   */
  private static final Class<? extends IndexDataProvider> DATAPROV_CLASS
          = AbstractIndexDataProviderTestImpl.class;

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public AbstractIndexDataProviderTest() throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.IndexSize.SMALL),
            DATAPROV_CLASS);
  }

  /**
   * Test of getDocumentIds method, of class AbstractIndexDataProvider.
   *
   * @see #testGetDocumentIds__plain()
   */
  public void testGetDocumentIds() {
    // implemented by super class
  }

  /**
   * Test of isTemporary method, of class AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testIsTemporary() throws Exception {
    final AbstractIndexDataProviderTestImpl instance
            = (AbstractIndexDataProviderTestImpl) IndexTestUtil.
            createInstance(
                    index, AbstractIndexDataProviderTestImpl.class,
                    null, null);
    boolean expResult = Environment.isTestRun();
    boolean result = instance.isTemporary();
    assertEquals("Temporary flag not set.", expResult, result);
  }

  /**
   * Test of setStopwordsFromEnvironment method, of class
   * AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetStopwordsFromEnvironment() throws Exception {
    final AbstractIndexDataProviderTestImpl instance
            = (AbstractIndexDataProviderTestImpl) IndexTestUtil.
            createInstance(
                    index, AbstractIndexDataProviderTestImpl.class,
                    null, IndexTestUtil.getRandomStopWords(index));
    instance.setStopwordsFromEnvironment();
  }

  /**
   * Test of warmUpTerms method, of class AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   * @see #testWarmupTerms__plain()
   */
  public void testWarmUpTerms() throws Exception {
    // implemented in super class
  }

  /**
   * Test of warmUpIndexTermFrequencies method, of class
   * AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Override
  public void testWarmUpIndexTermFrequencies() throws Exception {
    final AbstractIndexDataProviderTestImpl instance
            = (AbstractIndexDataProviderTestImpl) IndexTestUtil.
            createInstance(
                    index, AbstractIndexDataProviderTestImpl.class,
                    null, null);
    instance.warmUpIndexTermFrequencies();
  }

  /**
   * Test of warmUpDocumentIds method, of class AbstractIndexDataProvider.
   *
   * @see #testWarmUpDocumentIds__plain()
   */
  public void testWarmUpDocumentIds() {
    // implemented in super class
  }

  /**
   * Test of warmUpDocumentFrequencies method, of class
   * AbstractIndexDataProvider.
   *
   * @see #testWarmUpDocumentFrequencies__plain()
   */
  public void testWarmUpDocumentFrequencies() {
    // implemented by super class
  }

  /**
   * Test of warmUp method, of class AbstractIndexDataProvider.
   *
   * @see #testWarmUp__plain()
   */
  public void testWarmUp() {
    // implemented by super class
  }

  /**
   * Test of getPersistence method, of class AbstractIndexDataProvider.
   *
   * @see #testGetPersistence__plain()
   */
  public void testGetPersistence() {
    // implemented by super class
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider.
   *
   * @see #testGetFieldId__plain()
   */
  public void testGetFieldId() {
    // implemented in super class
  }

  /**
   * Test of _getTermFrequency method, of class AbstractIndexDataProvider.
   *
   * @see #test_getTermFrequency__plain()
   */
  public void test_getTermFrequency() {
    // implemented by super class
  }

  /**
   * Test of getTermFrequency method, of class AbstractIndexDataProvider.
   *
   * @see #testGetTermFrequency_ByteArray__plain()
   */
  public void testGetTermFrequency_ByteArray() {
    // implemented by super class
  }

  /**
   * Test method for getTermFrequency method, of class
   * AbstractIndexDataProvider.
   *
   * @see #testGetTermFrequency_0args__plain()
   */
  public void testGetTermFrequency_0args() {
    // implemented by super class
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Plain.
   *
   * @see #testGetTerms__plain()
   */
  public void testGetTerms() {
    // implemented by super class
  }

  /**
   * Test of clearCache method, of class AbstractIndexDataProvider.
   *
   * @see #testClearCache__plain()
   */
  public void testClearCache() {
    // implemented by super class
  }

  /**
   * Test of getRelativeTermFrequency method, of class
   * AbstractIndexDataProvider.
   *
   * @see #testGetRelativeTermFrequency__plain()
   */
  public void testGetRelativeTermFrequency() {
    // implemented by super class
  }

  /**
   * Test of dispose method, of class AbstractIndexDataProvider.
   *
   * @see testDispose__plain()
   */
  public void testDispose() {
    // implemented by super class
  }

  /**
   * Test of getTermsIterator method, of class AbstractIndexDataProvider.
   *
   * @see #testGetTermsIterator__plain()
   */
  public void testGetTermsIterator() {
    // implemented by super class
  }

  /**
   * Test of getTermsSource method, of class AbstractIndexDataProvider.
   *
   * @see #testGetTermsSource__plain()
   */
  public void testGetTermsSource() {
    // implemented by super class
  }

  /**
   * Test of getDocumentIdIterator method, of class AbstractIndexDataProvider.
   *
   * @see #testGetDocumentIdIterator__plain()
   */
  public void testGetDocumentIdIterator() {
    // implemented by super class
  }

  /**
   * Test of getDocumentIdSource method, of class AbstractIndexDataProvider.
   *
   * @see #testGetDocumentIdSource__plain()
   */
  public void testGetDocumentIdSource() {
    // implemented by super class
  }

  /**
   * Test of getUniqueTermsCount method, of class AbstractIndexDataProvider.
   *
   * @see #testGetUniqueTermsCount__plain()
   */
  public void testGetUniqueTermsCount() {
    // implemented by super class
  }

  /**
   * Test of hasDocument method, of class AbstractIndexDataProvider.
   *
   * @see #testHasDocument__plain()
   */
  public void testHasDocument() {
    // implemented by super class
  }

  /**
   * Test of getDocumentCount method, of class AbstractIndexDataProvider.
   *
   * @see #testGetDocumentCount__plain()
   */
  public void testGetDocumentCount() {
    // implemented by super class
  }

  /**
   * Simple testing implementation of {@link AbstractIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class AbstractIndexDataProviderTestImpl
          extends AbstractIndexDataProvider {

    /**
     * Fake collection of document ids.
     */
    static final Collection<Integer> DOC_IDS = Arrays.asList(new Integer[]{ 1,
      2, 3 });

    /**
     * Create a random named temporary {@link IndexDataProvider} instance.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public AbstractIndexDataProviderTestImpl() {
      super(RandomValue.getString(1, 15), true);
    }

    @Override
    public Collection<Integer> getDocumentIds() {
      return DOC_IDS;
    }

    @Override
    public void warmUpDocumentFrequencies() throws Exception {
      // NOP
    }

    @Override
    public int getDocumentFrequency(final ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentModel getDocumentModel(final int docId) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<ByteArray> getDocumentsTermSet(
            final Collection<Integer> docIds) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean documentContains(final int documentId, final ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void loadCache(final String name) throws Exception {
      // NOP
    }

    /**
     * Create needed data structures.
     */
    private void initCache() {
      super.db = DBMaker.newTempFileDB().make();
      super.idxDocumentIds = DOC_IDS;
      super.cachedFieldsMap = DbMakers.cachedFieldsMapMaker(super.db).make();
      byte fieldByte = 0;
      for (String field : Environment.getFields()) {
        super.cachedFieldsMap.put(field, new SerializableByte(fieldByte++));
      }
      super.idxTermsMap = DbMakers.idxTermsMapMkr(super.db).make();
      super.idxTerms = DbMakers.idxTermsMaker(super.db).make();
      super.idxTf = super.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).
              get();
    }

    @Override
    public void loadOrCreateCache(final String name) throws Exception {
      initCache();
    }

    @Override
    public void createCache(final String name) throws Exception {
      initCache();
    }
  }

}
