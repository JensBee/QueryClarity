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
package de.unihildesheim.lucene.index;

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IndexDataProvider} implementation directly accessing the Lucene
 * index.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DirectIndexDataProvider
        implements IndexDataProvider, Environment.FieldsChangedListener {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProvider.class);

  /**
   * Cached collection of all index terms.
   */
  private Collection<BytesWrap> idxTerms = null;
  /**
   * Cached collection of all (non deleted) document-ids.
   */
  private Collection<Integer> idxDocumentIds = null;
  /**
   * Cached term-frequency map for all terms in index.
   */
  private Map<BytesWrap, Long> idxTfMap = null;
  /**
   * Cached overall term frequency of the index.
   */
  private Long idxTf = null;
  /**
   * Persistent disk backed storage backend.
   */
  private DB db;
  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private boolean isTemporary = false;

  /**
   * Prefix used to store configuration.
   */
  private static final String IDENTIFIER = "DirectIDP";
  /**
   * Manager for external added document term-data values.
   */
  private ExternalDocTermDataManager externalTermData;

  /**
   * Default constructor using the parameters set by {@link Environment}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public DirectIndexDataProvider() throws IOException {
    this(false);
  }

  /**
   * Custom constructor allowing to set the parameters manually and optionally
   * creating a temporary instance.
   *
   * @param temporaray If true, all persistent data will be temporary
   * @throws IOException Thrown on low-level I/O errors
   */
  protected DirectIndexDataProvider(final boolean temporaray) throws
          IOException {
    this.isTemporary = temporaray;

    DBMaker dbMkr;
    if (this.isTemporary) {
      dbMkr = DBMaker.newTempFileDB();
    } else {
      dbMkr = DBMaker.newFileDB(
              new File(Environment.getDataPath(), IDENTIFIER));
    }
    dbMkr.cacheLRUEnable(); // enable last-recent-used cache
    dbMkr.cacheSoftRefEnable();
    this.db = dbMkr.make();
    this.externalTermData = new ExternalDocTermDataManager(this.db,
            IDENTIFIER);

    Environment.addFieldsChangedListener(this);
  }

  /**
   * Checks, if a document with the given id is known (in index).
   *
   * @param docId Document-id to check.
   */
  private void checkDocId(final int docId) {
    if (!hasDocument(docId)) {
      throw new IllegalArgumentException("No document with id '" + docId
              + "' found.");
    }
  }

  @Override
  public long getTermFrequency() {
    if (this.idxTf == null) {
      this.idxTf = 0L;
      for (String field : Environment.getFields()) {
        final long fieldTotalTf;
        try {
          fieldTotalTf = Environment.getIndexReader().getSumTotalTermFreq(
                  field);
          if (fieldTotalTf == -1) {
            throw new IllegalStateException("Error retrieving term frequency "
                    + "information for field '" + field + "'. Got "
                    + fieldTotalTf
                    + ".");
          }
          this.idxTf += fieldTotalTf;
        } catch (IOException ex) {
          LOG.error("Error retrieving term frequency information.", ex);
        }
      }
    }
    return this.idxTf;
  }

  @Override
  public Long getTermFrequency(final BytesWrap term) {
    if (this.idxTfMap == null) {
      this.idxTfMap = DBMaker.newTempHashMap();
    } else if (this.idxTfMap.containsKey(term)) {
      return this.idxTfMap.get(term);
    }

    long freq = 0;
    for (String field : Environment.getFields()) {
      try {
        DocsEnum de = MultiFields.
                getTermDocsEnum(Environment.getIndexReader(), MultiFields.
                        getLiveDocs(Environment.getIndexReader()), field,
                        new BytesRef(term.getBytes()));
        int docId = de.nextDoc();
        while (docId != DocsEnum.NO_MORE_DOCS) {
          freq += de.freq();
          docId = de.nextDoc();
        }
      } catch (IOException ex) {
        LOG.error("Error retrieving term frequency value.", ex);
      }
    }
    this.idxTfMap.put(term.clone(), freq);
    return freq;
  }

  @Override
  public double getRelativeTermFrequency(final BytesWrap term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    final double tf = getTermFrequency(term).doubleValue();
    if (tf == 0) {
      return 0d;
    }
    return tf / Long.valueOf(getTermFrequency());
  }

  @Override
  public void dispose() {
    if (Environment.isInitialized()) {
      Environment.removeFieldsChangedListener(this);
    }
  }

  /**
   * Collect and cache all index terms.
   *
   * @return Unique collection of all terms in index
   */
  private Collection<BytesWrap> getTerms() {
    if (this.idxTerms == null) {
      this.idxTerms = DBMaker.newTempTreeSet();
      try {
        final Fields idxFields = MultiFields.getFields(Environment.
                getIndexReader());
        if (idxFields == null) {
          throw new IllegalStateException("No fields retrieved.");
        } else {
          TermsEnum termsEnum = TermsEnum.EMPTY;
          for (String field : Environment.getFields()) {
            final Terms terms = idxFields.terms(field);
            if (terms == null) {
              LOG.warn("No terms in field '{}'.", field);
            } else {
              termsEnum = terms.iterator(termsEnum);
              BytesRef br = termsEnum.next();
              while (br != null) {
                this.idxTerms.add(new BytesWrap(br));
                br = termsEnum.next();
              }
            }
          }
        }
      } catch (IOException ex) {
        LOG.error("Error retrieving field information.", ex);
      }
    }
    return Collections.unmodifiableCollection(idxTerms);
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    return getTerms().iterator();
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    return new CollectionSource<>(getTerms());
  }

  /**
   * Collect and cache all document-ids from the index.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  private Collection<Integer> getDocumentIds() {
    if (this.idxDocumentIds == null) {
      this.idxDocumentIds = DBMaker.newTempTreeSet();
      final int maxDoc = Environment.getIndexReader().maxDoc();

      final Bits liveDocs = MultiFields.getLiveDocs(Environment.
              getIndexReader());
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          continue;
        }
        this.idxDocumentIds.add(i);
      }
    }
    return Collections.unmodifiableCollection(this.idxDocumentIds);
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    return getDocumentIds().iterator();
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public long getUniqueTermsCount() {
    return getTerms().size();
  }

  @Override
  public Object setTermData(final String prefix, final int documentId,
          final BytesWrap term,
          final String key, final Object value) {
    return this.externalTermData.setData(prefix, documentId, term, key,
            value);
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term,
          final String key) {
    return this.externalTermData.getData(prefix, documentId, term, key);
  }

  @Override
  public Map<BytesWrap, Object> getTermData(final String prefix,
          final int documentId,
          final String key) {
    return this.externalTermData.getData(prefix, documentId, key);
  }

  @Override
  public void clearTermData() {
    this.externalTermData.clear();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocId(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
            = new DocumentModel.DocumentModelBuilder(docId);

    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
      BytesRef br = dftEnum.next();
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Map<BytesWrap, AtomicLong> tfMap = new HashMap<>();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        if (tfMap.containsKey(bw)) {
          tfMap.get(bw).getAndAdd(dftEnum.getTotalTermFreq());
        } else {
          tfMap.put(bw.clone(), new AtomicLong(dftEnum.
                  getTotalTermFreq()));
        }
        br = dftEnum.next();
      }
      for (Entry<BytesWrap, AtomicLong> tfEntry : tfMap.entrySet()) {
        dmBuilder.setTermFrequency(tfEntry.getKey(), tfEntry.getValue().
                longValue());
      }
      return dmBuilder.getModel();
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
              + "docId=" + docId + ".", ex);
    }
    return null;
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    final int maxDoc = Environment.getIndexReader().maxDoc();

    if (docId <= (maxDoc - 1) && docId >= 0) {
      final Bits liveDocs = MultiFields.getLiveDocs(Environment.
              getIndexReader());
      return liveDocs == null || liveDocs.get(docId);
    }
    return false;
  }

  @Override
  public Collection<BytesWrap> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new HashSet<>();
    for (Integer docId : docIds) {
      checkDocId(docId);
      try {
        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
        BytesRef br = dftEnum.next();
        while (br != null) {
          terms.add(new BytesWrap(br));
          br = dftEnum.next();
        }
      } catch (IOException ex) {
        LOG.error("Caught exception while iterating document terms. "
                + "docId=" + docId + ".", ex);
      }
    }
    return terms;
  }

  @Override
  public long getDocumentCount() {
    return getDocumentIds().size();
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    checkDocId(documentId);
    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(documentId);
      BytesRef br = dftEnum.next();
      while (br != null) {
        if (new BytesWrap(br).equals(term)) {
          return true;
        }
        br = dftEnum.next();
      }
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
              + "docId=" + documentId + ".", ex);
    }
    return false;
  }

  @Override
  public void registerPrefix(final String prefix) {
    this.externalTermData.loadPrefix(prefix);
  }

  @Override
  public void fieldsChanged(final String[] oldFields, final String[] newFields) {
    LOG.debug("Fields changed, clearing cached data.");
    this.idxTerms = null;
    this.idxTfMap = null;
    this.idxTf = null;
  }
}
