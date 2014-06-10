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

import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.util.RandomValue;
import org.apache.lucene.index.IndexReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Test for {@link DirectIndexDataProvider}.
 *
 * @author Jens Bertram
 */
public final class DirectIndexDataProviderTest
    extends IndexDataProviderTestCase {

  /**
   * Initialize the test.
   *
   * @throws Exception Any exception indicates an error
   */
  public DirectIndexDataProviderTest()
      throws Exception {
    super(new TestIndexDataProvider());
  }

  @Override
  protected Class<? extends IndexDataProvider> getInstanceClass() {
    return DirectIndexDataProvider.class;
  }

  @Override
  protected DirectIndexDataProvider createInstance(final String dataDir,
      final IndexReader reader, final Set<String> fields,
      final Set<String> stopwords)
      throws Exception {
    return new DirectIndexDataProvider.Builder()
        .temporary()
        .dataPath(dataDir)
        .documentFields(fields)
        .indexReader(reader)
        .stopwords(stopwords)
        .createCache("test-" + RandomValue.getString(16))
        .warmUp()
        .build();
  }

  /**
   * Test, if the {@link DirectIndexDataProvider} handles a broken static index
   * with missing documents-terms-map correctly.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testBrokenCache_docTermsMap()
      throws Exception {
    final DirectIndexDataProvider instance = breakPersistentCache
        ("brokenDocTermsMap" + RandomValue.getString(16),
            DirectIndexDataProvider.CacheDbMakers.Stores.IDX_DOC_TERMS_MAP);
    Assert.assertFalse("Recovered docTermsMap is empty.",
        instance.getIdxDocTermsMap().isEmpty());
    instance.close();
  }

  /**
   * Simulates a broken index by removing a cache store from the index. The
   * instance is returned to provide access for further tests.
   *
   * @param cacheName Name of the cache file
   * @param storeKey Store key to remove
   * @return New Instance
   * @throws Exception Any exception thrown indicates an error
   */
  private DirectIndexDataProvider breakPersistentCache(
      final String cacheName,
      final DirectIndexDataProvider.CacheDbMakers.Stores storeKey)
      throws Exception {
    DirectIndexDataProvider instance;
    Persistence p;

    // create a new instance and delete the 'doc terms map' after warm-up
    instance = getLocalInstance(cacheName, false);
    p = instance.getPersistStatic();
    p.getDb().delete(storeKey.name());
    p.getDb().commit(); // write deleted data
    // check, if database object was removed
    if (p.getDb().exists(storeKey.name())) {
      Assert.fail("Database object was not removed by test case. " +
          "obj=" + storeKey);
    }
    instance.close(); // graceful close

    // re-open with corrupt database
    instance = getLocalInstance(cacheName, true);
    p = instance.getPersistStatic();
    // check, if database object was removed
    if (!p.getDb().exists(storeKey.name())) {
      Assert
          .fail("Database object was re-created by instance. obj=" + storeKey);
    }
    return instance;
  }

  /**
   * Creates a new instance for local testing methods.
   *
   * @param cacheName Name of the cache to create or get
   * @param readOnly If true, then the instance will re-open the persistent
   * cache in read-only mode after initialization
   * @return New instance
   * @throws Exception Any exception thrown indicates an error
   */
  private DirectIndexDataProvider getLocalInstance(final String cacheName,
      final boolean readOnly)
      throws Exception {
    final DirectIndexDataProvider.Builder builder =
        new DirectIndexDataProvider.Builder()
            .temporary()
            .dataPath(this.referenceIndex.reference().getDataDir())
            .documentFields(this.referenceIndex.reference().getDocumentFields())
            .indexReader(TestIndexDataProvider.getIndexReader())
            .loadOrCreateCache("test-local-" + cacheName)
            .warmUp();

    if (!readOnly) {
      builder.noReadOnly();
    }

    return builder.build();
  }

  /**
   * Test, if the {@link DirectIndexDataProvider} handles a broken static index
   * with missing indexed fields list correctly.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testBrokenCache_idxFields()
      throws Exception {
    final DirectIndexDataProvider instance = breakPersistentCache
        ("brokenIdxFields" + RandomValue.getString(16),
            DirectIndexDataProvider.CacheDbMakers.Stores.IDX_FIELDS);
    Assert.assertFalse("Recovered idxFieldsMap is empty.",
        instance.getCachedFieldsMap().isEmpty());
    instance.close();
  }

  /**
   * Test, if the {@link DirectIndexDataProvider} handles a broken static index
   * with missing index terms-map correctly.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testBrokenCache_idxTermsMap()
      throws Exception {
    final DirectIndexDataProvider instance = breakPersistentCache
        ("brokenIdxTermsMap" + RandomValue.getString(16),
            DirectIndexDataProvider.CacheDbMakers.Stores.IDX_TERMS_MAP);
    Assert.assertFalse("Recovered idxTermsMap is empty.",
        instance.getIdxTermsMap().isEmpty());
    instance.close();
  }
}
