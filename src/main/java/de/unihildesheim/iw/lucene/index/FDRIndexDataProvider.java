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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class FDRIndexDataProvider
    implements IndexDataProvider {
  /**
   * Context for high precision math calculations.
   */
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString()));
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
   * @throws DataProviderException Forwarded from {@link CollectionMetrics}
   * @throws IOException Thrown on low-level I/O-errors
   */
  private FDRIndexDataProvider(final Builder builder)
      throws IOException, DataProviderException {
    // first initialize the Lucene index
    assert builder.idxReader != null;

    LOG.info("Initializing index & gathering base data..");
    this.index = new LuceneIndex((FilteredDirectoryReader) builder.idxReader);

    // all data gathered, initialize metrics instance
    this.metrics = new CollectionMetrics(this);

    LOG.debug("index.TTF {} index.UT {}", this.index.ttf,
        this.index.uniqueTerms);
    LOG.debug("TTF (abwasserreinigungsstuf): {}", getTermFrequency(new
        ByteArray("abwasserreinigungsstuf"
        .getBytes(StandardCharsets.UTF_8))));
  }

  @Override
  public long getTermFrequency() {
    return this.index.ttf;
  }

  @Override
  public final Long getTermFrequency(final ByteArray term) {
    final BytesRef termBr = BytesRefUtils.fromByteArray(term);
    return this.index.reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .filter(r -> r.numDocs() > 0)
        .mapToLong(r -> {
          try {
            return StreamSupport.stream(r.fields().spliterator(), false)
                .mapToLong(f -> {
                  try {
                    final Terms terms = r.terms(f);
                    final TermsEnum termsEnum = terms.iterator(null);
                    if (termsEnum.seekExact(termBr)) {
                      return termsEnum.totalTermFreq();
                    } else {
                      return 0L;
                    }
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                .sum();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).sum();
//    return this.index.fields.stream()
//        .mapToLong(f -> {
//          try {
//            final Terms terms = MultiFields.getTerms(this.index.reader, f);
//            final TermsEnum termsEnum = terms.iterator(null);
//            if (termsEnum.seekExact(BytesRefUtils.fromByteArray(term))) {
//              return termsEnum.totalTermFreq();
//            } else {
//              return 0L;
//            }
//          } catch (final IOException e) {
//            throw new UncheckedIOException(e);
//          }
//        }).sum();
  }

  @Override
  public int getDocumentFrequency(final ByteArray term) {
    final BytesRef termBr = BytesRefUtils.fromByteArray(term);
    return this.index.reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .filter(r -> r.numDocs() > 0)
        .mapToInt(r -> {
          try {
            return StreamSupport.stream(r.fields().spliterator(), false)
                .mapToInt(f -> {
                  try {
                    final Terms terms = r.terms(f);
                    final TermsEnum termsEnum = terms.iterator(null);
                    if (termsEnum.seekExact(termBr)) {
                      return termsEnum.docFreq();
                    } else {
                      return 0;
                    }
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
                .sum();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).sum();

//    return this.index.fields.stream()
//        .mapToInt(f -> {
//          try {
//            final Terms terms = MultiFields.getTerms(this.index.reader, f);
//            final TermsEnum termsEnum = terms.iterator(null);
//            if (termsEnum.seekExact(BytesRefUtils.fromByteArray(term))) {
//              return termsEnum.docFreq();
//            } else {
//              return 0;
//            }
//          } catch (final IOException e) {
//            throw new UncheckedIOException(e);
//          }
//        }).sum();
  }

  @Override
  public Stream<Integer> getDocumentIds() {
    return this.index.docIds.stream();
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    return new DocumentModel.Builder(docId)
        .setTermFrequency(getDocumentTerms(docId)).getModel();
  }

  /**
   * Get a mapping (or list) of all terms in a specific document.
   *
   * @param docId Document id
   * @return List of terms or mapping of term to (within document) term
   * frequency value
   */
  private Map<ByteArray, Long> getDocumentTerms(final int docId) {
    return this.index.fields.stream()
        .flatMap(f -> {
          try {
            final Terms terms = this.index.reader.getTermVector(docId, f);
            if (terms == null) {
              LOG.warn("No Term Vectors for field {} in document {}.", f);
              LOG.debug("Field exists? {}",
                  (this.index.reader.document(docId).getField(f) != null));
              return Stream.empty();
            } else {
              final TermsEnum te = terms.iterator(null);
              return StreamSupport.stream(new Spliterator<ByteArray>() {
                @Override
                public boolean tryAdvance(
                    final Consumer<? super ByteArray> action) {
                  try {
                    final BytesRef nextTerm = te.next();
                    if (nextTerm == null) {
                      return false;
                    } else {
                      action.accept(BytesRefUtils.toByteArray(nextTerm));
                      return true;
                    }
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }

                @Override
                @Nullable
                public Spliterator<ByteArray> trySplit() {
                  return null; // no split support
                }

                @Override
                public long estimateSize() {
                  return Long.MAX_VALUE; // we don't know
                }

                @Override
                public int characteristics() {
                  return IMMUTABLE; // not mutable
                }
              }, false);
            }
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).collect(HashMap<ByteArray, Long>::new,
            (map, ba) -> {
              if (map.containsKey(ba)) {
                map.put(ba, map.get(ba) + 1L);
              } else {
                map.put(ba, 1L);
              }
            }, HashMap<ByteArray, Long>::putAll);
  }

  @Override
  public boolean hasDocument(final int docId) {
    return this.index.docIds.contains(docId);
  }

  @Override
  public Stream<ByteArray> getDocumentsTerms(final Set<Integer> docIds) {
    return docIds.stream()
        .map(docId -> {
          try {
            return this.index.reader.getTermVectors(docId);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .filter(f -> f != null)
        .map(f -> {
          final Set<ByteArray> termSet = new HashSet<>(100 * docIds.size());
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
                  while (true) {
                    final BytesRef term = te.next();
                    if (term == null) {
                      break;
                    }
                    termSet.add(BytesRefUtils.toByteArray(term));
                  }
                } catch (final IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
          return termSet;
        })
        .collect(HashSet<ByteArray>::new,
            AbstractCollection::addAll,
            HashSet<ByteArray>::addAll)
        .stream();
  }

  @Override
  public long getDocumentCount() {
    return (long) this.index.docCount;
  }

  @Override
  public Set<String> getDocumentFields() {
    return new HashSet<>(this.index.fields);
  }

  @Override
  public Set<String> getStopwords() {
    return Collections.emptySet();
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
     * {@link IndexReader} to access the Lucene index.
     */
    private final FilteredDirectoryReader reader;
    /**
     * List of document-id of visible documents.
     */
    private final Collection<Integer> docIds;
    /**
     * Number of documents visible.
     */
    private final int docCount;
    /**
     * Frequency of all terms in index.
     */
    private final long ttf;
    /**
     * Number of unique terms in index (respects active fields).
     */
    private final long uniqueTerms;
    /**
     * Document fields.
     */
    private final Collection<String> fields;

    /**
     * Initialize the index information store.
     *
     * @param r IndexReader
     * @throws IOException Thrown on low-level I/O-errors
     */
    LuceneIndex(final FilteredDirectoryReader r)
        throws IOException {
      this.reader = r;

      // collect the ids of all documents in the index
      LOG.debug("Estimating index size");
      final int numDocs = this.reader.numDocs();
      LOG.info("Collecting all ({}) documents from index.", numDocs);
      final Query q = new MatchAllDocsQuery();
      final IndexSearcher searcher = new IndexSearcher(this.reader);
      final TopDocs matches = searcher.search(q, numDocs);
      LOG.debug("Query returned {} matching documents.", matches.totalHits);
      this.docIds = Arrays.stream(matches.scoreDocs)
          .map(sd -> sd.doc)
          .collect(Collectors.toList());
      this.docCount = this.docIds.size();
      // both counts should be equal, since there are no deletions
      assert this.docCount == numDocs;

      // collect summed total term frequency of all terms in the index
      LOG.info("Collecting term counts (TTF)");
      final Fields fields = MultiFields.getFields(this.reader);
      if (fields == null) {
        LOG.warn("Reader does not contain any postings.");
        this.ttf = 0L;
        this.fields = Collections.emptyList();
      } else {
        this.fields = StreamSupport.stream(fields.spliterator(), false)
            .collect(Collectors.toList());
        this.ttf = this.fields.stream()
            .mapToLong(f -> {
              try {
                return this.reader.getSumTotalTermFreq(f);
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }).sum();
      }

      // check for TermVectors
      if (!this.fields.isEmpty()) {
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
      if (this.fields.isEmpty()) {
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
        this.uniqueTerms = this.fields.stream()
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
                final TermsEnum te = mTerms.iterator(null);
                return StreamSupport.stream(new Spliterator<BytesRef>() {
                  @Override
                  public boolean tryAdvance(
                      final Consumer<? super BytesRef> action) {
                    try {
                      final BytesRef nextTerm = te.next();
                      if (nextTerm == null) {
                        return false;
                      } else {
                        action.accept(nextTerm);
                        return true;
                      }
                    } catch (final IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  }

                  @Override
                  @Nullable
                  public Spliterator<BytesRef> trySplit() {
                    return null; // no split support
                  }

                  @Override
                  public long estimateSize() {
                    return Long.MAX_VALUE; // we don't know
                  }

                  @Override
                  public int characteristics() {
                    return IMMUTABLE; // not mutable
                  }
                }, false);
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }).distinct().count();

//        ReaderSlice[] rSlices = this.reader.leaves().stream()
//            .filter(arc -> arc.reader().numDocs() > 0)
//            .map(arc -> new ReaderSlice(
//                arc.docBase, arc.reader().maxDoc(), fields.size() - 1))
//            .toArray(ReaderSlice[]::new);


//        this.uniqueTerms = this.fields.stream()
//            .flatMap(f -> {
//              try {
//                final TermsEnum te = MultiFields.getTerms(this.reader, f)
//                    .iterator(null);
//                return StreamSupport.stream(new Spliterator<BytesRef>() {
//                  @Override
//                  public boolean tryAdvance(
//                      final Consumer<? super BytesRef> action) {
//                    try {
//                      final BytesRef nextTerm = te.next();
//                      if (nextTerm == null) {
//                        return false;
//                      } else {
//                        action.accept(nextTerm);
//                        return true;
//                      }
//                    } catch (final IOException e) {
//                      throw new UncheckedIOException(e);
//                    }
//                  }
//
//                  @Override
//                  @Nullable
//                  public Spliterator<BytesRef> trySplit() {
//                    return null; // no split support
//                  }
//
//                  @Override
//                  public long estimateSize() {
//                    return Long.MAX_VALUE; // we don't know
//                  }
//
//                  @Override
//                  public int characteristics() {
//                    return IMMUTABLE; // not mutable
//                  }
//                }, false);
//              } catch (final IOException e) {
//                throw new UncheckedIOException(e);
//              }
//            }).distinct().count();
      }
    }
  }

  /**
   * Builder for creating a new {@link LuceneIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Constructor setting the supported features.
     */
    public Builder() {
      super(new Feature[]{
          Feature.COMMON_TERM_THRESHOLD
      });
    }

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    public FDRIndexDataProvider build()
        throws BuildException, ConfigurationException {
      validate();
      assert this.idxReader != null;
      if (!FilteredDirectoryReader.class.isInstance(this.idxReader)) {
        throw new BuildException(
            "IndexReader must be an instance of FilteredDirectoryReader.");
      }
      if (this.idxReader.hasDeletions()) {
        throw new BuildException(
            "Index with deletions is currently not supported.");
      }

      try {
        return new FDRIndexDataProvider(this);
      } catch (final DataProviderException | IOException e) {
        throw new BuildException("Failed to create instance.", e);
      }
    }
  }
}
