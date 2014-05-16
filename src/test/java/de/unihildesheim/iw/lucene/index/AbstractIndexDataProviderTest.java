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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import org.apache.lucene.index.IndexReader;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Test for {@link AbstractIndexDataProvider}.
 *
 * @author Jens Bertram
 */
public final class AbstractIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public AbstractIndexDataProviderTest()
      throws Exception {
    super(new TestIndexDataProvider(TestIndexDataProvider.DEFAULT_INDEX_SIZE));
  }

  /**
   * Test of isTemporary method, of class AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Ignore
  public void testIsTemporary()
      throws Exception {
//    final AbstractIndexDataProviderTestImpl instance
//        = (AbstractIndexDataProviderTestImpl) IndexTestUtil.
//        createInstance(
//            referenceIndex, AbstractIndexDataProviderTestImpl.class,
//            null, null);
//    boolean expResult = Environment.isTestRun();
//    boolean result = instance.isTemporary();
//    assertEquals("Temporary flag not set.", expResult, result);
  }

  @Override
  protected Class<? extends IndexDataProvider> getInstanceClass() {
    return AbstractIndexDataProviderTestImpl.class;
  }

  @Override
  protected IndexDataProvider createInstance(final String dataDir,
      final IndexReader reader, final Set<String> fields,
      final Set<String> stopwords)
      throws UnsupportedEncodingException {
    final AbstractIndexDataProviderTestImpl instance = new
        AbstractIndexDataProviderTestImpl();
    instance.setStopwords(stopwords);
    instance.setDocumentFields(fields);
    instance.setIdxTerms(Collections.<ByteArray>emptySet());
    instance.setIdxTf(1L);
    instance.setDb(DBMaker.newTempFileDB().make());
    instance.setIdxTermsMap(
        new ConcurrentSkipListMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long>()
    );
    instance.setCachedFieldsMap(
        new HashMap<String, SerializableByte>(fields.size())
    );
    for (final String field : fields) {
      instance.addFieldToCacheMap(field);
    }
    return instance;
  }

  /**
   * Test of warmUpIndexTermFrequencies method, of class
   * AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @Override
  @Ignore
  public void testWarmUpIndexTermFrequencies()
      throws Exception {
//    final AbstractIndexDataProviderTestImpl instance
//        = (AbstractIndexDataProviderTestImpl) IndexTestUtil.
//        createInstance(
//            referenceIndex, AbstractIndexDataProviderTestImpl.class,
//            null, null);
//    instance.warmUpIndexTermFrequencies();
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
    static final Collection<Integer> DOC_IDS = Arrays.asList(1, 2, 3);

    /**
     * Create a random named temporary {@link IndexDataProvider} instance.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public AbstractIndexDataProviderTestImpl() {
      super(true);
    }


    @Override
    public Collection<Integer> getDocumentIds() {
      return DOC_IDS;
    }

    @Override
    public void warmUpDocumentFrequencies() {
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
    public Set<ByteArray> getDocumentsTermSet(
        final Collection<Integer> docIds) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean documentContains(final int documentId,
        final ByteArray term) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

}
