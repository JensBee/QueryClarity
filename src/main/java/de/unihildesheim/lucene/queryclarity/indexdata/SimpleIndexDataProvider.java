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
package de.unihildesheim.lucene.queryclarity.indexdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class SimpleIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          SimpleIndexDataProvider.class);

  /**
   * Reader instance for the target lucene index.
   */
  private final IndexReader reader;

  /**
   * Store frequency of each term in Lucene's index.
   */
  private final Map<String, Long> termFreq;

  /**
   * Size of the cache for relative term frequencies.
   */
  private final int REL_TERM_FREQ_CACHE_SIZE = 1000;

  /**
   * Store previously calculated relative term frequencies.
   */
  private final Map<String, Double> relativeTermFreq = new LinkedHashMap() {
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > REL_TERM_FREQ_CACHE_SIZE;
    }
  };

  private final Map<String, Map<Integer, Double>> docTermFreq = new HashMap();

  /**
   * Lucene fields to operate on.
   */
  private final Collection<String> targetFields;

  /**
   * Create a new {@link AbstractIndexDataProvider} from an existing lucene
   * index in the local file system.
   *
   * @param index Lucene index directory location
   * @param fields Lucene fields to operate on
   * @throws IOException Thrown, if the index could not be opened
   * @throws de.unihildesheim.lucene.queryclarity.indexData.IndexDataException
   * Thrown, if not all requested fields are present in the index
   */
  public SimpleIndexDataProvider(final IndexReader indexReader,
          final String[] fields) throws IOException, IndexDataException {
    super();
    // HashSet removes any possible duplicates
    this.targetFields = new HashSet(Arrays.asList(fields));
    // init term -> frequency storage
    this.termFreq = new HashMap();

    this.reader = indexReader;

    LOG.info("SimpleIndexDataProvider instance reader={} fields={}.", reader,
            fields);

    // get all indexed fields from index - other fields are not of interes here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
            this.reader);

    // pre-check if all requested fields are available
    if (!indexedFields.containsAll(this.targetFields)) {
      throw new IndexDataException(IndexDataException.Type.FIELD_NOT_PRESENT,
              this.targetFields, indexedFields);
    }

    calculateTermFrequencies();
  }

  /**
   * Calculate term frequencies for all terms in the index (in the initial given
   * fields).
   *
   * @throws IOException Thrown, if the index could not be opened
   */
  private final void calculateTermFrequencies() throws IOException {
    long startTime = System.nanoTime();
    final Fields idxFields = MultiFields.getFields(this.reader);

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null;
    BytesRef bytesRef;
    String term;

    // go through all fields..
    for (String field : this.targetFields) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them,,
        while ((bytesRef = fieldTermsEnum.next()) != null) {
          term = bytesRef.utf8ToString();

          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            LOG.debug("term={}, freq={}", term, fieldTermsEnum.totalTermFreq());

            // ..and get the frequency value for term + field
            if (termFreq.containsKey(term)) {
              termFreq.put(term, termFreq.get(term) + fieldTermsEnum.
                      totalTermFreq());
            } else {
              termFreq.put(term, fieldTermsEnum.totalTermFreq());
            }
          }
        }
      }
    }
    double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.info("Calculation of term frequencies for all {} terms in index "
            + "took {} seconds.", termFreq.size(), estimatedTime);
  }

  @Override
  public Set<String> getTerms() {
    return termFreq.keySet();
  }

  @Override
  public long getTermFrequency() {
    long termFrequency = 0L;
    for (long freq : termFreq.values()) {
      termFrequency += freq;
    }
    return termFrequency;
  }

  @Override
  public long getTermFrequency(String term) {
    Long freq = termFreq.get(term);
    if (freq == null) {
      freq = 0L;
    }
    return freq;
  }

  @Override
  public double getRelativeTermFrequency(String term) {
    Double rTermFreq = relativeTermFreq.get(term);

    if (rTermFreq == null) {
      LOG.trace("[pC(t)] t={}", term);

      final long tFreq = getTermFrequency(term);
      final long cFreq = getTermFrequency();
      if (tFreq > 0 && cFreq > 0) {
        rTermFreq = (double) tFreq / (double) cFreq;
      }
      relativeTermFreq.put(term, rTermFreq);
      LOG.debug("[pC(t)] t={} p={}", term, rTermFreq);
    }

    // term was not found in the index
    if (rTermFreq == null) {
      rTermFreq = 0d;
    }

    return rTermFreq;
  }

  /**
   * Close this instance.
   */
  @Override
  public void dispose() {
    try {
      this.reader.close();
    } catch (IOException e) {
      LOG.error("Error while disposing instance.", e);
    }
  }

  @Override
  public String[] getTargetFields() {
    return this.targetFields.toArray(new String[this.targetFields.size()]);
  }
}
