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

import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Configuration;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
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
public final class DirectIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProvider.class);

  private final String[] fields;
  private final IndexReader reader;
  private Collection<BytesWrap> idxTerms = null;
  private Collection<Integer> idxDocumentIds = null;
  private Map<BytesWrap, Long> idxTfMap = null;
  private Long idxTf = null;
  private DB db;

  private boolean isTemporary = false;

  /**
   * Directory where cached data should be stored.
   */
  private final String storagePath;
  /**
   * Unique identifier for this cache.
   */
  private final String storageId;
  /**
   * Storage meta data.
   */
  private transient Properties storageProp = null;

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "DirectIDP_";
  /**
   * Manager for external added document term-data values.
   */
  private ExternalDocTermDataManager externalTermData;

  public DirectIndexDataProvider(final String newStorageId,
          final IndexReader indexReader, final String[] newFields,
          final boolean temporaray) throws IOException {
    this.fields = newFields.clone();
    this.reader = indexReader;
    this.isTemporary = temporaray;

    DBMaker dbMkr;
    this.storageId = newStorageId;
    this.storagePath = Configuration.get(CONF_PREFIX + "storagePath",
            "data/cache/");
    if (this.isTemporary) {
      dbMkr = DBMaker.newTempFileDB();
    } else {
      dbMkr = DBMaker.newFileDB(new File(this.storagePath, this.storageId));
    }
    dbMkr.cacheLRUEnable(); // enable last-recent-used cache
    dbMkr.cacheSoftRefEnable();
    this.db = dbMkr.make();
    this.externalTermData = new ExternalDocTermDataManager(this.db,
            CONF_PREFIX);

    if (!this.isTemporary) {
      final File propFile = new File(this.storagePath, this.storageId
              + ".properties");
      try {
        try (FileInputStream propStream = new FileInputStream(propFile)) {
          this.storageProp.load(propStream);
        }
      } catch (FileNotFoundException ex) {
        LOG.trace("Cache meta file " + propFile + " not found.", ex);
      }
    }
  }

  @Override
  public long getTermFrequency() {
    if (this.idxTf == null) {
      this.idxTf = 0L;
      for (String field : this.fields) {
        final long fieldTotalTf;
        try {
          fieldTotalTf = this.reader.getSumTotalTermFreq(field);
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
    for (String field : this.fields) {
      try {
        DocsEnum de = MultiFields.getTermDocsEnum(this.reader, MultiFields.
                getLiveDocs(reader), field, new BytesRef(term.getBytes()));
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
    try {
      this.reader.close();
    } catch (IOException ex) {
      LOG.error("Caught exception while closing reader.", ex);
    }
  }

  private Collection<BytesWrap> getTerms() {
    if (this.idxTerms == null) {
      this.idxTerms = DBMaker.newTempTreeSet();
      try {
        final Fields idxFields = MultiFields.getFields(reader);
        if (idxFields == null) {
          throw new IllegalStateException("No fields retrieved.");
        } else {
          TermsEnum termsEnum = TermsEnum.EMPTY;
          for (String field : this.fields) {
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
    return idxTerms;
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    return getTerms().iterator();
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    return new CollectionSource<>(getTerms());
  }

  private Collection<Integer> getDocumentIds() {
    if (this.idxDocumentIds == null) {
      this.idxDocumentIds = DBMaker.newTempTreeSet();
      final int maxDoc = this.reader.maxDoc();

      final Bits liveDocs = MultiFields.getLiveDocs(reader);
      for (int i = 0; i < reader.maxDoc(); i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          continue;
        }
        this.idxDocumentIds.add(i);
      }
    }
    return this.idxDocumentIds;
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term,
          final String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<BytesWrap, Object> getTermData(final String prefix,
          final int documentId,
          final String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clearTermData() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    if (this.hasDocument(docId)) {
      final DocumentModel.DocumentModelBuilder dmBuilder
              = new DocumentModel.DocumentModelBuilder(docId);

      try {
        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(this.reader,
                this.fields, docId);
        BytesRef br = dftEnum.next();
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        final Map<BytesWrap, Number> tfMap = new HashMap<>();
        while (br != null) {
          final BytesWrap bw = new BytesWrap(br);
          if (tfMap.containsKey(bw)) {
            ((AtomicLong) tfMap.get(bw)).getAndAdd(dftEnum.getTotalTermFreq());
          } else {
            tfMap.put(bw.clone(), new AtomicLong(dftEnum.
                    getTotalTermFreq()));
          }
          br = dftEnum.next();
        }
        dmBuilder.setTermFrequency(tfMap);
        return dmBuilder.getModel();
      } catch (IOException ex) {
        LOG.error("Caught exception while iterating document terms. "
                + "docId=" + docId + ".", ex);
      }
    }
    return null;
  }

  @Override
  public boolean addDocument(final DocumentModel docModel) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    final int maxDoc = this.reader.maxDoc();

    if (docId <= (maxDoc - 1) && docId >= 0) {
      final Bits liveDocs = MultiFields.getLiveDocs(this.reader);
      return liveDocs == null || liveDocs.get(docId);
    }
    return false;
  }

  @Override
  public Collection<BytesWrap> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getDocumentCount() {
    return getDocumentIds().size();
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
