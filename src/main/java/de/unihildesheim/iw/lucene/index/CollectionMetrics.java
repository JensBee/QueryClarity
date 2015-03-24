/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.mapdb.serializer.BytesRefSerializer;
import de.unihildesheim.iw.mapdb.serializer.DocumentModelSerializer;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.Map;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class CollectionMetrics {
  /**
   * Index total term frequency value.
   */
  private final long tf;
  /**
   * Number of documents in index.
   */
  private final long docCount;
  /**
   * Cache document frequency values.
   */
  private final Map<BytesRef, Integer> c_df;
  /**
   * Cache term frequency values.
   */
  private final Map<BytesRef, Long> c_tf;
  /**
   * Cache relative term frequency values.
   */
  private final Map<BytesRef, Double> c_rtf;
  /**
   * Cache for created {@link DocumentModel}s.
   */
  private final Map<Integer, DocumentModel> c_docModel;
  /**
   * Data provider to access index data.
   */
  private final IndexDataProvider dataProv;

  /**
   * Initialize the collection data provider - optionally caching some basic
   * index information, based on settings made in the configuration object.
   *
   * @param idp DataProvider
   */
  public <I extends IndexDataProvider> CollectionMetrics(@NotNull final I idp) {
    this.dataProv = idp;
    this.tf = this.dataProv.getTermFrequency();
    this.docCount = this.dataProv.getDocumentCount();

    this.c_rtf = DBMaker
        .newMemoryDirectDB()
        .transactionDisable()
        .make()
        .createHashMap("cache")
        .keySerializer(BytesRefSerializer.SERIALIZER)
        .valueSerializer(Serializer.BASIC)
        .expireStoreSize(1500d)
        .make();

    this.c_docModel = DBMaker
        .newMemoryDirectDB()
        .transactionDisable()
        .make()
        .createHashMap("cache")
        .keySerializer(Serializer.INTEGER)
        .valueSerializer(DocumentModelSerializer.SERIALIZER)
        .expireStoreSize(1000d)
        .make();

    this.c_df = DBMaker
        .newMemoryDirectDB()
        .transactionDisable()
        .make()
        .createHashMap("cache")
        .keySerializer(BytesRefSerializer.SERIALIZER)
        .valueSerializer(Serializer.INTEGER)
        .expireStoreSize(1500d)
        .make();

    this.c_tf = DBMaker
        .newMemoryDirectDB()
        .transactionDisable()
        .make()
        .createHashMap("cache")
        .keySerializer(BytesRefSerializer.SERIALIZER)
        .valueSerializer(Serializer.LONG)
        .expireStoreSize(1500d)
        .make();
  }

  /**
   * Get the raw frequency of a given term in the collection.
   *
   * @param term Term to lookup
   * @return Collection frequency of the given term
   */
  public long tf(final BytesRef term) {
    @Nullable Long result = this.c_tf.get(term);
    if (result == null) {
      // may return null, if term is not known
      result = this.dataProv.getTermFrequency(term);
      this.c_tf.put(BytesRef.deepCopyOf(term), result);
    }
    return result;
  }

  /**
   * Get the relative frequency of a term. The relative frequency is the
   * frequency {@code tF} of term {@code t} divided by the frequency {@code F}
   * of all terms.
   *
   * @param term Term to lookup
   * @return Relative collection frequency of the given term
   */
  public double relTf(final BytesRef term) {
    @Nullable Double result = this.c_rtf.get(term);
    if (result == null) {
      final long tf = tf(term);
      result = tf == 0L ? 0d : (double) tf / (double) this.tf;
      this.c_rtf.put(BytesRef.deepCopyOf(term), result);
    }
    return result;
  }

  /**
   * Get the document frequency of a term.
   *
   * @param term Term to lookup.
   * @return Document frequency of the given term
   */
  public Integer df(final BytesRef term) {
    @Nullable Integer result = this.c_df.get(term);
    if (result == null) {
      result = this.dataProv.getDocumentFrequency(term);
      this.c_df.put(BytesRef.deepCopyOf(term), result);
    }
    return result;
  }

  /**
   * Get the relative document frequency of a term. The relative document
   * frequency of a term is defined as {@code reldf(term) = df(term) /
   * number_of_documents}.
   *
   * @param term Term to lookup.
   * @return Document frequency of the given term
   */
  public double relDf(final BytesRef term) {
    return df(term).doubleValue() / (double) this.docCount;
  }

  /**
   * Get a document data model from the {@link IndexDataProvider}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document-model for the given document id
   */
  public DocumentModel docData(final int documentId) {
    @Nullable DocumentModel d = this.c_docModel.get(documentId);
    if (d == null) {
      d = this.dataProv.getDocumentModel(documentId);
      this.c_docModel.put(documentId, d);
    }
    return d;
  }
}
