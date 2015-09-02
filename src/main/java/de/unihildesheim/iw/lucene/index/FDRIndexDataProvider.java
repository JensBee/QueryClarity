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
import de.unihildesheim.iw.lucene.util.BytesRefUtils.MergingBytesRefHash;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.Buildable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections4.map.LRUMap;
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
import org.apache.lucene.util.BitDocIdSet;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
   * Size of the document-model LRU cache.
   */
  static final int CACHE_DOCMOD_SIZE = 10000;
  /**
   * LRU cache of document models.
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "CloneableClassWithoutClone", "serial"})
  private final Map<Integer, DocumentModel> cache_docmod =
      Collections.synchronizedMap(
          new LRUMap<>(CACHE_DOCMOD_SIZE + 1, 0.75f)
//          new LinkedHashMap<Integer, DocumentModel>(
//              CACHE_DOCMOD_SIZE + 1, .75F, true) {
//            @Override
//            public boolean removeEldestEntry(final Map.Entry eldest) {
//              return size() > CACHE_DOCMOD_SIZE;
//            }
//          }
      );
  /**
   * Size of the term-frequency LRU cache.
   */
  static final int CACHE_TF_SIZE = 500000;
  /**
   * LRU cache of term-frequency values.
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "CloneableClassWithoutClone", "serial"})
  private final Map<BytesRef, Long> cache_tf = Collections.synchronizedMap(
      new LRUMap<>(CACHE_TF_SIZE + 1, 0.75F));
  /**
   * Size of the document-frequency LRU cache.
   */
  static final int CACHE_DF_SIZE = 10000;
  /**
   * LRU cache of document-frequency values.
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "CloneableClassWithoutClone", "serial"})
  private final Map<BytesRef, Integer> cache_df = Collections.synchronizedMap(
      new LRUMap<>(CACHE_DF_SIZE + 1, 0.75F));

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

  @Override
  public Optional<Long> getUniqueTermCount() {
    return Optional.of(this.index.uniqueTerms);
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  public long getTermFrequency(@NotNull final BytesRef term) {
    // try get a cached value first
    @Nullable
    Long tf = this.cache_tf.get(term);
    if (tf == null) {
      final AtomicLong newTf = new AtomicLong(0);
      this.index.readerLeaves.stream().parallel()
          .map(LeafReaderContext::reader)
          .filter(r -> r.numDocs() > 0)
          .forEach(r -> {
            try {
              for (final String s : r.fields()) {
                @Nullable final Terms terms = r.terms(s);
                if (terms != null) {
                  final TermsEnum termsEnum = terms.iterator(null);
                  if (termsEnum.seekExact(term)) {
                    newTf.addAndGet(termsEnum.totalTermFreq());
                  }
                }
              }
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          });
      tf = newTf.get();
      this.cache_tf.put(BytesRef.deepCopyOf(term), tf);
    }

    return tf;
  }

  @Override
  public double getRelativeTermFrequency(@NotNull final BytesRef term) {
    final long tf = getTermFrequency(term);
    return tf == 0L ? 0d : (double) tf / (double) this.index.ttf;
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  public int getDocumentFrequency(@NotNull final BytesRef term) {
    Integer df = this.cache_df.get(term);
    if (df == null) {
      df = this.index.reader.leaves().stream()
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
                      return termsEnum.seekExact(term) ? termsEnum.docFreq() :
                          0;
                    } catch (final IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  })
                  .max().orElse(0);
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          }).sum();
      this.cache_df.put(BytesRef.deepCopyOf(term), df);
    }
    return df;
  }

  @Override
  public double getRelativeDocumentFrequency(final BytesRef term) {
    return (double) getDocumentFrequency(term) / (double) this.index.docCount;
  }

  @Override
  @NotNull
  public IntStream getDocumentIds() {
    return StreamUtils.stream(this.index.docIds);
  }

  @Override
  @NotNull
  public DocIdSet getDocumentIdSet() {
    return new BitDocIdSet(this.index.docIds.clone());
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @NotNull
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    DocumentModel dm = this.cache_docmod.get(docId);
    if (dm == null) {
      dm = new DocumentModel.Builder(docId)
          .setTermFrequency(getDocumentTerms(docId)).build();
      this.cache_docmod.put(docId, dm);
    }
    return dm;
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
      LOG.warn("No fields or TermVectors! docId={}", docId);
      return Stream.empty();
    }

    final BytesRefHash terms = new BytesRefHash();
    StreamSupport.stream(fields.spliterator(), false)
//        .peek(fn -> LOG.info("@field {}", fn))
        // filter for required fields
        .filter(fn -> Arrays.binarySearch(field, fn) >= 0)
//        .peek(fn -> LOG.info("@field {}, get terms", fn))
        .map(fn -> {
          try {
            return fields.terms(fn);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        })
//        .peek(t -> LOG.info("@terms not null? {}", t != null))
        .filter(t -> t != null)
        .forEach(t -> {
          try {
//            LOG.info("@terms - iterating ({})", t.size());
            final TermsEnum te = t.iterator(null);
            BytesRef term;
            while ((term = te.next()) != null) {
//              LOG.info("@terms, found term {}", term.utf8ToString());
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
     * {@link IndexReader}'s leaves.
     */
    final List<LeafReaderContext> readerLeaves;
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
      this.readerLeaves = r.leaves();

      // collect the ids of all documents in the index
      if (LOG.isDebugEnabled()) {
        LOG.debug("Estimating index size");
      }
      final int numDocs = this.reader.numDocs();

      if (numDocs <= 0) {
        throw new IllegalStateException("Index is empty");
      }

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
      @Nullable final Fields fields = MultiFields.getFields(this.reader);
      if (fields == null) {
        LOG.warn("Reader does not contain any postings.");
        this.ttf = 0L;
        this.fields = NO_FIELDS;
      } else {
        this.fields = StreamSupport.stream(fields.spliterator(), false)
            .toArray(String[]::new);
        this.ttf = Arrays.stream(this.fields)
            .peek(f -> LOG.info("Collecting term counts (TTF) field={}", f))
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
            this.readerLeaves.stream()
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
      if (this.fields.length == 0) {
        // still no postings
        this.uniqueTerms = 0L;
      } else {
        // gather sub-reader which have documents
        final LeafReaderContext[] leaves =
            this.readerLeaves.stream()
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
            .peek(f -> LOG
                .info("Collecting term counts (unique terms) field={}", f))
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
