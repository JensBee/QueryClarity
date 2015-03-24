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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils.MergingBytesRefHash;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class FDRIndexDataProvider
    implements IndexDataProvider {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(FDRIndexDataProvider.class);
  /**
   * Object wrapping Lucene index information.
   */
  private final LuceneIndex index;
  /**
   * Collection metrics instance for this DataProvider.
   */
  private final CollectionMetrics metrics;

  /**
   * Create instance by using {@link Builder}.
   *
   * @param builder Builder instance
   * @throws IOException Thrown on low-level I/O-errors
   */
  @SuppressWarnings("WeakerAccess")
  FDRIndexDataProvider(final Builder builder)
      throws IOException {
    // first initialize the Lucene index
    assert builder.idxReader != null;

    LOG.info("Initializing index & gathering base data..");
    this.index = new LuceneIndex(builder.idxReader);

    // all data gathered, initialize metrics instance
    this.metrics = new CollectionMetrics(this);

    if (LOG.isDebugEnabled()) {
      LOG.debug("index.TTF {} index.UT {}", this.index.ttf,
          this.index.uniqueTerms);
      LOG.debug("TTF (abwasserreinigungsstuf): {}", getTermFrequency(new
          BytesRef("abwasserreinigungsstuf")));
    }
  }

  @Override
  public long getTermFrequency() {
    return this.index.ttf;
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  public long getTermFrequency(@NotNull final BytesRef term) {
    return this.index.reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .filter(r -> r.numDocs() > 0)
        .mapToLong(r -> {
          try {
            return StreamSupport.stream(r.fields().spliterator(), false)
                .mapToLong(f -> {
                  try {
                    @Nullable final Terms terms = r.terms(f);
                    if (terms == null) {
                      return 0L;
                    }
                    final TermsEnum termsEnum = terms.iterator(null);
                    return termsEnum.seekExact(term) ?
                        termsEnum.totalTermFreq() : 0L;
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                .sum();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).sum();
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  public int getDocumentFrequency(@NotNull final BytesRef term) {
    return this.index.reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .filter(r -> r.numDocs() > 0)
        .mapToInt(r -> {
          try {
            return StreamSupport.stream(r.fields().spliterator(), false)
                .mapToInt(f -> {
                  try {
                    @Nullable final Terms terms = r.terms(f);
                    if (terms == null) {
                      return 0;
                    }
                    final TermsEnum termsEnum = terms.iterator(null);
                    return termsEnum.seekExact(term) ? termsEnum.docFreq() : 0;
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                .max().orElse(0);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).sum();
  }

  @Override
  @NotNull
  public IntStream getDocumentIds() {
    return StreamUtils.stream(this.index.docIds);
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @NotNull
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    return new DocumentModel.Builder(docId)
        .setTermFrequency(getDocumentTerms(docId)).build();
  }

  /**
   * Get a mapping (or list) of all terms in a specific document.
   *
   * @param docId Document id
   * @return List of terms or mapping of term to (within document) term
   * frequency value
   */
  private Map<BytesRef, Long> getDocumentTerms(final int docId) {
    return Arrays.stream(this.index.fields)
        .flatMap(f -> {
          try {
            @Nullable final Terms terms =
                this.index.reader.getTermVector(docId, f);
            if (terms == null) {
              LOG.warn("No Term Vectors for field {} in document {}.",
                  f, docId);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Field exists? {}",
                    this.index.reader.document(docId).getField(f) != null);
              }
              return Stream.empty();
            } else {
              return StreamUtils.stream(terms.iterator(null));
            }
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).collect(HashMap<BytesRef, Long>::new,
            (map, ba) -> {
              if (map.containsKey(ba)) {
                map.put(ba, map.get(ba) + 1L);
              } else {
                map.put(ba, 1L);
              }
            }, HashMap<BytesRef, Long>::putAll);
  }

  @Override
  public boolean hasDocument(final int docId) {
    return docId < this.index.docIds.length() && this.index.docIds.get(docId);
  }

  @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS",
      "EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
  @Override
  public Stream<BytesRef> getDocumentTerms(final int docId,
      @NotNull final String... field) {
    Arrays.sort(field);
    final Fields fields;
    try {
      fields = this.index.reader.getTermVectors(docId);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    if (fields == null) {
      return Stream.empty();
    }

    final BytesRefHash terms = new BytesRefHash();
    StreamSupport.stream(fields.spliterator(), false)
        // filter for required fields
        .filter(fn -> Arrays.binarySearch(field, fn) >= 0)
        .map(fn -> {
          try {
            return fields.terms(fn);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .filter(t -> t != null)
        .forEach(t -> {
          try {
            final TermsEnum te = t.iterator(null);
            BytesRef term;
            while ((term = te.next()) != null) {
              terms.add(term);
            }
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    return StreamUtils.stream(terms);
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  @NotNull
  public Stream<BytesRef> getDocumentsTerms(@NotNull final DocIdSet docIds) {
    try {
      return StreamUtils.stream(docIds)
          .mapToObj(docId -> {
            try {
              return this.index.reader.getTermVectors(docId);
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          })
          .filter(f -> f != null)
          .map(f -> {
            final BytesRefHash terms = new BytesRefHash();
            StreamSupport.stream(f.spliterator(), false)
                .map(fn -> {
                  try {
                    return f.terms(fn);
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                .filter(t -> t != null)
                .forEach(t -> {
                  try {
                    final TermsEnum te = t.iterator(null);
                    BytesRef term;
                    while ((term = te.next()) != null) {
                      terms.add(term);
                    }
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                });
            return terms;
          })
          .collect(MergingBytesRefHash::new,
              MergingBytesRefHash::addAll,
              MergingBytesRefHash::addAll)
          .stream();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long getDocumentCount() {
    return (long) this.index.docCount;
  }

  @Override
  @NotNull
  public String[] getDocumentFields() {
    return this.index.fields;
  }

  @Override
  public CollectionMetrics metrics() {
    return this.metrics;
  }

  /**
   * Information about the provided Lucene index.
   */
  private static final class LuceneIndex {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(LuceneIndex.class);
    /**
     * {@link IndexReader} to access the Lucene index.
     */
    final FilteredDirectoryReader reader;
    /**
     * List of document-id of visible documents.
     */
    final FixedBitSet docIds;
    /**
     * Number of documents visible.
     */
    final int docCount;
    /**
     * Frequency of all terms in index.
     */
    final long ttf;
    /**
     * Number of unique terms in index (respects active fields).
     */
    final long uniqueTerms;
    /**
     * Document fields.
     */
    final String[] fields;
    /**
     * Constant value if no document fields are available.
     */
    private static final String[] NO_FIELDS = new String[0];

    /**
     * Initialize the index information store.
     *
     * @param r IndexReader
     * @throws IOException Thrown on low-level I/O-errors
     */
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    LuceneIndex(@NotNull final FilteredDirectoryReader r)
        throws IOException {
      this.reader = r;

      // collect the ids of all documents in the index
      if (LOG.isDebugEnabled()) {
        LOG.debug("Estimating index size");
      }
      final int numDocs = this.reader.numDocs();

      LOG.info("Collecting all ({}) documents from index.", numDocs);
      final Query q = new MatchAllDocsQuery();
      final IndexSearcher searcher = IndexUtils.getSearcher(this.reader);
      final TopDocs matches = searcher.search(q, numDocs);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Query returned {} matching documents.", matches.totalHits);
      }
      final int[] docIds = Arrays.stream(matches.scoreDocs)
          .mapToInt(sd -> sd.doc)
          .sorted()
          .toArray();
      this.docIds = new FixedBitSet(docIds[docIds.length - 1] + 1);
      Arrays.stream(docIds)
          .forEach(this.docIds::set);
      this.docCount = this.docIds.cardinality();
      if (LOG.isDebugEnabled()) {
        LOG.debug("DocIds c={}", this.docCount);
      }
      // both counts should be equal, since there are no deletions
      assert this.docCount == numDocs;

      // collect summed total term frequency of all terms in the index
      LOG.info("Collecting term counts (TTF)");
      @Nullable final Fields fields = MultiFields.getFields(this.reader);
      if (fields == null) {
        LOG.warn("Reader does not contain any postings.");
        this.ttf = 0L;
        this.fields = NO_FIELDS;
      } else {
        this.fields = StreamSupport.stream(fields.spliterator(), false)
            .toArray(String[]::new);
        this.ttf = Arrays.stream(this.fields)
            .mapToLong(f -> {
              try {
                return this.reader.getSumTotalTermFreq(f);
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }).sum();
      }

      // check for TermVectors
      if (this.fields.length > 0) {
        final boolean termVectorsMissing =
            this.reader.getContext().leaves().stream()
                .filter(arc -> arc.reader().getFieldInfos().size() > 0)
                .flatMap(
                    arc -> StreamSupport.stream(
                        arc.reader().getFieldInfos().spliterator(), false))
                .filter(fi -> {
                  if (!fi.hasVectors()) {
                    LOG.error("TermVector missing. f={}", fi.name);
                    return true;
                  }
                  return false;
                })
                .findFirst()
                .isPresent();
        if (termVectorsMissing) {
          throw new IllegalStateException(
              "TermVectors are not present for all fields.");
        }
      }

      // collect all unique terms from the index
      LOG.info("Collecting term counts (unique terms)");
      if (this.fields.length == 0) {
        // still no postings
        this.uniqueTerms = 0L;
      } else {
        // gather sub-reader which have documents
        final LeafReaderContext[] leaves =
            this.reader.leaves().stream()
                .filter(lrc -> lrc.reader().numDocs() > 0)
                .toArray(LeafReaderContext[]::new);

        // collect slices for all sub-readers
        final ReaderSlice[] slices = IntStream.range(0, leaves.length)
            // create slice for each sub-reader
            .mapToObj(i -> new ReaderSlice(
                leaves[i].docBase, leaves[i].reader().maxDoc(), i))
            .toArray(ReaderSlice[]::new);

        // iterate all terms in all fields in all sub-readers
        this.uniqueTerms = Arrays.stream(this.fields)
            // collect terms instances from all sub-readers
            .map(f -> IntStream.range(0, leaves.length)
                .mapToObj(i -> {
                  try {
                    return leaves[i].reader().terms(f);
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                    // exclude empty terms
                .filter(t -> t != null)
                .toArray(Terms[]::new))
            .flatMap(t -> {
              try {
                final MultiTerms mTerms = new MultiTerms(t, slices);
                return StreamUtils.stream(mTerms.iterator(null));
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }).distinct().count();
      }
    }
  }

  /**
   * Builder for creating a new {@link FDRIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<FDRIndexDataProvider> {
    /**
     * {@link FilteredDirectoryReader} to use for accessing the Lucene index.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    @Nullable
    FilteredDirectoryReader idxReader;

    /**
     * Set the {@link FilteredDirectoryReader} to use.
     *
     * @param reader {@link FilteredDirectoryReader} instance
     * @return Self reference
     */
    public Builder indexReader(
        @NotNull final FilteredDirectoryReader reader) {
      this.idxReader = reader;
      return this;
    }

    /**
     * Create a new {@link FDRIndexDataProvider} instance.
     *
     * @return new {@link FDRIndexDataProvider} instance
     * @throws BuildException Thrown on any error during construction
     * @throws ConfigurationException Thrown on configuration errors
     */
    @NotNull
    @Override
    public FDRIndexDataProvider build()
        throws BuildException, ConfigurationException {
      validate();
      try {
        return new FDRIndexDataProvider(this);
      } catch (final IOException e) {
        throw new BuildException("Failed to create instance.", e);
      }
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.idxReader == null) {
        throw new ConfigurationException("IndexReader not set.");
      }
      if (!FilteredDirectoryReader.class.isInstance(this.idxReader)) {
        throw new ConfigurationException(
            "IndexReader must be an instance of FilteredDirectoryReader.");
      }
    }
  }
}
