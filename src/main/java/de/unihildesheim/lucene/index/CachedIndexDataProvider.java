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
import de.unihildesheim.Persistence;
import de.unihildesheim.SerializableByte;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesRefUtil;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * INCOMPLETE!
 *
 * @author Jens Bertram
 */
public final class CachedIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CachedIndexDataProvider.class);

  /**
   * Persistent map storing (document, field, term -> frequency) mappings.
   */
  private ConcurrentNavigableMap<Fun.Tuple3<
          Integer, SerializableByte, ByteArray>, Long> idxDocTermsMap;

  /**
   * Named cache to use.
   */
  private String cacheName = null;

  /**
   * Names of objects stored in persistent database.
   */
  private enum Stores {

    /**
     * Document-> terms mapping.
     */
    IDX_DOC_TERMS_MAP,
    /**
     * List of document ids.
     */
    DOC_IDS
  }

  /**
   * Prefix used to store configuration.
   */
  public static final String IDENTIFIER = "CachedIDP";

  /**
   * Object handling persistence data storage.
   */
  private Persistence persistence;

  /**
   * Create a new instance with default settings.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public CachedIndexDataProvider() throws IOException {
    super(IDENTIFIER, false);
  }

  /**
   * Get a builder instance to create persistent data storage.
   *
   * @return Builder instance
   */
  private Persistence.Builder getPersistenceBuilder() {
    if (this.cacheName == null) {
      throw new IllegalStateException("Cache name not set.");
    }
    final Persistence.Builder psb;
    psb = new Persistence.Builder(IDENTIFIER + "_" + this.cacheName);
    psb.getMaker()
            .transactionDisable()
            .commitFileSyncDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(100)
            .mmapFileEnableIfSupported()
            .closeOnJvmShutdown();
    return psb;
  }

  /**
   * Get a {@link Persistence} instance to handle persistent data storage.
   *
   * @return {@link Persistence} instance
   * @throws FileNotFoundException Thrown, if cache file was not found
   * @throws Environment.NoIndexException Thrown, if no index is set in the
   * {@link Environment}
   */
  private Persistence getPersistence() throws FileNotFoundException,
          Environment.NoIndexException {
    if (this.cacheName == null) {
      throw new IllegalStateException("Cache name not set.");
    }
    if (this.persistence == null) {
      this.persistence = getPersistenceBuilder().get();
    }
    return this.persistence;
  }

  /**
   * Create the data configuration for persistent storing of document ids.
   *
   * @param db Database reference
   */
  private void createDocIdsSet(final DB db) {
    super.idxDocumentIds = db.createTreeSet(Stores.DOC_IDS.name())
            .serializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
            .counterEnable()
            .make();
  }

  /**
   * Creates the persistent cache.
   *
   * @param name Name of the cache
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in
   * the {@link Environment}
   */
  public void cacheBuilder(final String name) throws IOException,
          Environment.NoIndexException {
    this.cacheName = name;
    final Persistence p = getPersistenceBuilder().make();

    final BTreeKeySerializer idxDocTermsMapKeySerializer
            = new BTreeKeySerializer.Tuple3KeySerializer<>(
                    null, SerializableByte.COMPARATOR,
                    Serializer.INTEGER,
                    SerializableByte.SERIALIZER,
                    ByteArray.SERIALIZER);
    this.idxDocTermsMap = p.db.createTreeMap(Stores.IDX_DOC_TERMS_MAP.name())
            .keySerializer(idxDocTermsMapKeySerializer)
            .valueSerializer(Serializer.LONG)
            .counterEnable()
            .make();
    createDocIdsSet(p.db);

    if (!Environment.getStopwords().isEmpty()) {
      LOG.warn("Stopwords are set in environment. "
              + "Make sure, this is intended.");
    }

//    this.dIdp.createCache(IDENTIFIER + "_" + name);
//    this.dIdp.warmUp();
    buildCache();
  }

  @Override
  public void warmUp() throws Exception {
    this.idxDocTermsMap = getPersistence().db.get(Stores.IDX_DOC_TERMS_MAP.
            name());
    if (this.idxDocTermsMap.isEmpty()) {
      throw new IllegalStateException("Document terms map is empty. "
              + "A complete rebuild is needed.");
    }
    LOG.info("Cache warming: document terms map with {} entries loaded.",
            this.idxDocTermsMap.size());
    if (!getPersistence().db.exists(Stores.DOC_IDS.name())) {
      createDocIdsSet(getPersistence().db);
    } else {
      super.idxDocumentIds = getPersistence().db.get(Stores.DOC_IDS.name());
    }
    if (super.idxDocumentIds == null || super.idxDocumentIds.isEmpty()) {
      LOG.info("Document id list is empty. List will be rebuild.");
    } else {
      LOG.info("Cache warming: document id list with {} entries loaded.",
              this.idxDocumentIds.size());
    }

    super.warmUp();
  }

  /**
   * Calls processing methods to create the cache.
   *
   * @throws Environment.NoIndexException Thrown, if no index is set in the
   * {@link Environment}
   */
  private void buildCache() throws Environment.NoIndexException {
    final List<AtomicReaderContext> arContexts = Environment.getIndexReader().
            getContext().leaves();
    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(arContexts),
                    new DocTermsMapBuilderTarget(this.idxDocTermsMap,
                            (Set<Integer>) super.idxDocumentIds)
            )).process(arContexts.size());
  }

  @Override
  protected Collection<Integer> getDocumentIds() {
    if (super.idxDocumentIds.isEmpty()) {
      LOG.info("Building document-id list.");
      final NavigableSet<Fun.Tuple3<Integer, SerializableByte, ByteArray>> keys
              = this.idxDocTermsMap.keySet();

      @SuppressWarnings("BoxingBoxedValue")
      Fun.Tuple3<Integer, SerializableByte, ByteArray> current = keys.higher(
              Fun.t3(
                      (Integer) null, (SerializableByte) null,
                      (ByteArray) null)
      );

      while (current != null) {
        super.idxDocumentIds.add(current.a);
        current = keys.higher(Fun.t3(current.a, Fun.<SerializableByte>HI(),
                Fun.<ByteArray>HI()));
      }
    }
    return super.idxDocumentIds;
  }

  @Override
  protected void warmUpDocumentFrequencies() {
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
    // load the data from the wrapped instance
    super.getPersistence(DirectIndexDataProvider.IDENTIFIER + "_" + IDENTIFIER
            + "_" + name, false, false);
    boolean fail = false;

    if (!DbMakers.checkCaches(super.db).isEmpty()) {
      LOG.error("Cache is incomplete! Missing caches: {}", DbMakers.
              checkCaches(super.db));
      fail = true;
    }
    if (!DbMakers.checkStores(super.db).isEmpty()) {
      LOG.error("Cache is incomplete! Missing stores: {}", DbMakers.
              checkStores(super.db));
      fail = true;
    }

    if (fail) {
      throw new IllegalStateException("Cache is incomplete.");
    }

    super.cachedFieldsMap = DbMakers.cachedFieldsMapMaker(db).makeOrGet();
    super.idxTermsMap = DbMakers.idxTermsMapMkr(super.db).makeOrGet();
    super.idxTerms = DbMakers.idxTermsMaker(super.db).makeOrGet();
    super.idxTf = super.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).
            get();
    super.idxDfMap = DbMakers.idxDfMapMaker(super.db).makeOrGet();
    this.cacheName = name;
  }

  @Override
  public void loadOrCreateCache(final String name) throws Exception {
    throw new UnsupportedOperationException(
            "Use cacheBuilder function to create a cache.");
  }

  @Override
  public void createCache(final String name) throws Exception {
    if (name.startsWith(Environment.Builder.DEFAULT_CACHE_NAME)) {
      LOG.warn("Won't create a cache file. "
              + "Use cacheBuilder function to create a cache.");
    }
    throw new UnsupportedOperationException(
            "Use cacheBuilder function to create a cache.");
  }

  @Override
  public void dispose() {
    if (this.persistence != null) {
      try {
        LOG.info("Updating cache.");
        getPersistence().db.commit();
        getPersistence().db.close();
      } catch (FileNotFoundException | Environment.NoIndexException ex) {
        LOG.error("Error updating cache.", ex);
      }

    }
  }

  /**
   * {@link Processing} {@link Target} collecting document ids and creating a
   * document -> term mapping.
   */
  private final class DocTermsMapBuilderTarget extends
          Target.TargetFunc<AtomicReaderContext> {

    /**
     * Map to put results into.
     */
    private final Map<
            Fun.Tuple3<Integer, SerializableByte, ByteArray>, Long> map;
    /**
     * Collected document ids.
     */
    private final Set<Integer> docSet;

    /**
     * Create a new {@link Processing} {@link Target}.
     *
     * @param targetMap Target map for collected term results
     * @param targetSet Target for collected document ids
     */
    DocTermsMapBuilderTarget(
            final Map<Fun.Tuple3<
                    Integer, SerializableByte, ByteArray>, Long> targetMap,
            final Set<Integer> targetSet) {
      this.map = targetMap;
      this.docSet = targetSet;
    }

    @Override
    public void call(final AtomicReaderContext rContext) {
      if (rContext == null) {
        return;
      }
      final AtomicReader reader = rContext.reader();
      final int maxDoc = reader.maxDoc();
      DocFieldsTermsEnum dftEnum;
      BytesRef bytesRef;

      for (String field : Environment.getFields()) {
        try {
          dftEnum = new DocFieldsTermsEnum(reader, new String[]{field});
        } catch (IOException ex) {
          LOG.error("Error reading field. field={}", field, ex);
          continue;
        }

        final Bits liveDocs;
        try {
          liveDocs = reader.getDocsWithField(field);
        } catch (IOException ex) {
          LOG.error("Error getting documents for field. field={}", field, ex);
          continue;
        }
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs != null && !liveDocs.get(i)) {
            continue;
          }
          this.docSet.add(i); // store document id
          try {
            dftEnum.setDocument(i);
            bytesRef = dftEnum.next();
            while (bytesRef != null) {
              final ByteArray byteArray = BytesRefUtil.toByteArray(bytesRef);
              this.map.put(Fun.t3(i, CachedIndexDataProvider.super.getFieldId(
                      field), byteArray), dftEnum.getTotalTermFreq());
              bytesRef = dftEnum.next();
            }
          } catch (IOException ex) {
            LOG.error("Error reading document. id={}", i, ex);
          }
        }
      }
    }
  }
}
