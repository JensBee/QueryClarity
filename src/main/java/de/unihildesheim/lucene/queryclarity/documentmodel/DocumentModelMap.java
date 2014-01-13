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
package de.unihildesheim.lucene.queryclarity.documentmodel;

import de.unihildesheim.lucene.queryclarity.indexdata.AbstractIndexDataProvider;
import de.unihildesheim.lucene.queryclarity.indexdata.DocFieldsTermsEnum;
import java.util.HashMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DocumentModelMap extends HashMap<Integer, AbstractDocumentModel> {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocumentModelMap.class);

  /**
   * Reusable {@link DocFieldsTermsEnum} instance.
   */
  private final DocFieldsTermsEnum dftEnum;

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private double langModelWeight = 0.6d;

  /**
   * Data provider for cacheable index statistics.
   */
  private final AbstractIndexDataProvider dataProv;

  /**
   * Creates a new {@link DocumentModelMap} using the specified
   * {@link AbstractIndexDataProvider} instance for general index staistics and
   * the {@link DocFieldsTermsEnum} instance to iterate over single document
   * terms.
   *
   * The backing {@link HashMap} will be initialized with default values. If
   * this is not desired you may use the {@link DocumentModelMap#DocumentModelMap(AbstractIndexDataProvider,
   * DocFieldsTermEnum, int)} constructor alternatively.
   *
   * To get meaningful results, make sure the {@link AbstractIndexDataProvider}
   * and the {@link DocFieldsTermsEnum} are pointing at the same index fields.
   *
   * @param dataProvider Provider for general index statistics
   * @param docFieldsTermEnum Term enumerator for all document fields
   */
  public DocumentModelMap(final AbstractIndexDataProvider dataProvider,
          final DocFieldsTermsEnum docFieldsTermEnum) {
    super();
    dftEnum = docFieldsTermEnum;
    dataProv = dataProvider;
  }

  /**
   * Same as the {@link DocumentModelMap#DocumentModelMap(AbstractIndexDataProvider,
   * DocFieldsTermEnum)} constructor, but allows to set an initial capacity for
   * the backing {@link HashMap}.
   *
   * @see DocumentModelMap#DocumentModelMap(AbstractIndexDataProvider,
   * initialCapacity DocFieldsTermEnum)
   * @param dataProvider Provider for general index statistics
   * @param docFieldsTermEnum Term enumerator for all document fields
   * @param initialCapacity Initial capacity for the backing {@link HashMap}
   */
  public DocumentModelMap(final AbstractIndexDataProvider dataProvider,
          DocFieldsTermsEnum docFieldsTermEnum, final int initialCapacity) {
    super(initialCapacity);
    dftEnum = docFieldsTermEnum;
    dataProv = dataProvider;
  }

  /**
   * Creates a new {@link DocumentModelMap} while using the specified
   * {@link IndexReader} to construct a new {@link DocFieldsTermsEnum}. The
   * {@link DocFieldsTermsEnum} will use the same index fields as specified for
   * the {@link AbstractIndexDataProvider}.
   *
   * @see DocumentModelMap#DocumentModelMap(AbstractIndexDataProvider,
   * DocFieldsTermEnum)
   * @param indexReader {@link IndexReader} used to construct a new
   * {@link DocFieldsTermsEnum}
   * @param dataProvider Provider for general index statistics
   */
  public DocumentModelMap(final IndexReader indexReader,
          final AbstractIndexDataProvider dataProvider) {
    this(dataProvider, new DocFieldsTermsEnum(indexReader, dataProvider.
            getTargetFields()));
  }

  /**
   * Same as the {@link DocumentModelMap#DocumentModelMap(IndexReader,
   * AbstractIndexDataProvider)} constructor, but allows to set an initial
   * capacity for the backing {@link HashMap}.
   *
   * @see DocumentModelMap#DocumentModelMap(IndexReader,
   * AbstractIndexDataProvider)
   * @param indexReader {@link IndexReader} used to construct a new
   * {@link DocFieldsTermsEnum}
   * @param dataProvider Provider for general index statistics
   * @param initialCapacity Initial capacity for the backing {@link HashMap}
   */
  public DocumentModelMap(final IndexReader indexReader,
          final AbstractIndexDataProvider dataProvider,
          final int initialCapacity) {
    this(dataProvider, new DocFieldsTermsEnum(indexReader, dataProvider.
            getTargetFields()), initialCapacity);
  }

  @Override
  public boolean containsKey(final Object key) {
    boolean found = false;
    if (key instanceof Integer) {
      found = super.containsKey(key);
    }
    return found;
  }

  /**
   * Tries to add a new document, specified by the given id to the map. If the
   * document-id is already contained in the map nothing will happen. If it's
   * not already there, then a new {@link AbstractDocumentModel} for the
   * document specified by the given id will be created.
   *
   * @param key The document identified by it's id to add
   * @return The already stored document model, or a newly created model, if it
   * was not already created
   */
  public final AbstractDocumentModel put(final Integer key) {
    AbstractDocumentModel docModel = super.get(key);
    if (docModel == null) {
      docModel = new MapDocumentModel(key);
      super.put(key, docModel);
    }
    return docModel;
  }

  /**
   * Get a {@link Collection} view of the {@link AbstractDocumentModel}
   * instances contained in this map.
   *
   * @return {@link Collection} of the {@link AbstractDocumentModel} instances
   * contained in this map
   */
  @Override
  public final Collection<AbstractDocumentModel> values() {
    return super.values();
  }

  /**
   * Get the ids of all documents contained in this map.
   *
   * @return Ids of all documents contained in this map.
   */
  @Override
  public final Set<Integer> keySet() {
    return super.keySet();
  }

  /**
   * Document model implementation hold by this map.
   */
  private class MapDocumentModel extends AbstractDocumentModel {

    /**
     * Store association for query-term to probability.
     */
    private final Map<String, Double> probabilities = new HashMap();

    /**
     * Overall term frequency in this document. Set to <code>-1</code> if not
     * calculated.
     */
    private long overallTermFreq = -1l;

    MapDocumentModel(int documentId) {
      super(documentId);
    }

    /**
     * Store the probability for a query term for this document.
     *
     * @param term Query term
     * @param probability Probability for the query term
     * @return previously stored value or null, if none was set
     */
    private double addProbability(final String term,
            final double probability) {
      return probabilities.put(term, probability);
    }

    @Override
    public Double termProbability(final String term) {
      Double prob = probabilities.get(term);
      if (prob == null) {
        prob = super.termProbability(term);
        // value may be null on low-level errors
        if (prob != null) {
          probabilities.put(term, prob);
        }
      }
      return prob;
    }

    @Override
    public long getTermFrequency() {
      // check if this value was already calculated
      if (overallTermFreq == -1l) {
        // no term specified, count all term frequencies
        overallTermFreq = getTermFrequency(null);
      }
      return overallTermFreq;
    }

    @Override
    protected DocFieldsTermsEnum getDocFieldsTermsEnum() {
      return DocumentModelMap.this.dftEnum;
    }

    @Override
    protected double getLangmodelWeight() {
      return DocumentModelMap.this.langModelWeight;
    }

    @Override
    protected AbstractIndexDataProvider getAbstractIndexDataProvider() {
      return DocumentModelMap.this.dataProv;
    }
  }
}
