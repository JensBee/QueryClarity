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

import de.unihildesheim.iw.lucene.search.EmptyFieldFilter;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator.OfLong;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <ul> <li>only indices without deletions are supported</li> <li>{@code
 * numDeletedDocs()} will report false values, if filters are in effect</li>
 * </p>
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class FilteredDirectoryReader
    extends FilterDirectoryReader {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(FilteredDirectoryReader.class);
  /**
   * Filter to reduce the number of documents visible to this reader.
   */
  @Nullable
  private final Filter filter;
  /**
   * List of fields visible to the reader.
   */
  private final Set<String> fields;
  /**
   * If true, given fields should be negated.
   */
  private final boolean negateFields;
  /**
   * Term filter to use.
   */
  private final TermFilter termFilter;
  /**
   * Sub-reader wrapper instance.
   */
  private final SubReaderWrapper subWrapper;

  /**
   * @param dirReader Reader to wrap
   * @param wrapper Wrapper for sub-readers of the main reader
   * @param vFields Fields visible to the reader
   * @param negate If true, given fields should be negated
   * @param qFilter Filter to reduce the number of documents visible to the
   * reader
   * @param tFilter Term-filter
   */
  private FilteredDirectoryReader(
      final DirectoryReader dirReader, final SubReaderWrapper wrapper,
      final Collection<String> vFields, final boolean negate,
      @Nullable final Filter qFilter, final TermFilter tFilter) {
    // all sub-readers get initialized when calling super
    super(dirReader, wrapper);

    if (dirReader.hasDeletions()) {
      throw new IllegalStateException(
          "Indices with deletions are not supported.");
    }
    // all sub-readers are now initialized

    if (LOG.isDebugEnabled()) {
      final StringBuilder fInfo = new StringBuilder("Filter: [");
      if (!vFields.isEmpty()) {
        fInfo.append(" fields ");
      }
      if (qFilter != null) {
        fInfo.append(" query-filter ");
      }
      if (!AcceptAllTermFilter.class.isInstance(tFilter)) {
        fInfo.append(" term-filter ");
      }
      LOG.debug(fInfo.append(']').toString());
    }

    this.subWrapper = wrapper;
    this.negateFields = negate;
    this.filter = qFilter;

    // collect visible fields from all sub-readers
    this.fields = this.getSequentialSubReaders().stream()
        // skip readers without documents
        .filter(r -> r.numDocs() > 0)
        .flatMap(r -> {
          try {
            return StreamSupport.stream(r.fields().spliterator(), false);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }).collect(Collectors.toSet());
    LOG.debug("Fields: {}", this.fields);

//    if (vFields.isEmpty()) {
//      this.fields = Collections.emptySet();
//    } else if (vFields.size() == 1) {
//      this.fields = Collections.singleton(vFields.toArray(new String[1])[0]);
//    } else {
//      this.fields = new HashSet<>(vFields.size());
//      this.fields.addAll(vFields);
//    }

    this.termFilter = tFilter;
    this.termFilter.setTopReader(this);
  }

  /**
   * Returns the original DirectoryReader instance wrapped.
   *
   * @return original DirectoryReader instance wrapped
   */
  public DirectoryReader unwrap() {
    return this.in;
  }

  @Override
  public boolean hasDeletions() {
    // for sure, because we don't support indices with deleted documents
    return false;
  }

  @Override
  protected final DirectoryReader doWrapDirectoryReader(
      final DirectoryReader dirReader) {
    return new FilteredDirectoryReader(dirReader, this.subWrapper,
        this.fields, this.negateFields, this.filter, this.termFilter);
  }

  /**
   * Filtered {@link LeafReader} that wraps another AtomicReader and provides
   * filtering functions.
   */
  private static final class FilteredLeafReader
      extends FilterLeafReader {
    /**
     * Fields instance.
     */
    private final FilteredFields fieldsInstance;
    /**
     * Set of field names visible.
     */
    private final Set<String> fields;
    /**
     * If true, given fields should be negated.
     */
    private final boolean negateFields;
    /**
     * Contextual meta-data.
     */
    private final FLRContext flrContext;
    /**
     * FieldInfos reduced to the visible fields.
     */
    private FieldInfos fieldInfos;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterAtomicReader is
     * closed.</p>
     *
     * @param lReader specified base reader
     * @param vFields Collection of fields visible to the reader
     * @param negate If true, given fields should be negated
     * @param qFilter Filter to reduce the number of documents visible to this
     * reader
     * @param tFilter Term-filter
     * @throws IOException Thrown on low-level I/O-errors
     */
    FilteredLeafReader(final LeafReader lReader,
        final Collection<String> vFields, final boolean negate,
        @Nullable final Filter qFilter, final TermFilter tFilter)
        throws IOException {
      super(lReader);

      this.negateFields = negate;

      if (vFields.isEmpty() && qFilter == null) {
        LOG.warn("No filters specified. " +
            "You should use a plain IndexReader to get better performance.");
      }

      this.flrContext = new FLRContext();

      // all docs are initially live (no deletions allowed)
      this.flrContext.docBits = new FixedBitSet(this.in.maxDoc());
      this.flrContext.docBits.set(0, this.flrContext.docBits.length());

      // reduce the number of visible fields, if desired
      if (vFields.isEmpty()) {
        // all fields are visible
        this.fieldInfos = this.in.getFieldInfos();
        this.fields = Collections.emptySet();
      } else {
        this.fields = new HashSet<>(vFields.size());
        this.fields.addAll(vFields);
        // filter gets applied
        applyFieldFilter(vFields);
      }

      if (qFilter != null) {
        // user document filter gets applied
        applyDocFilter(qFilter);
      }

      // set maxDoc value
      if (this.fields.isEmpty() && qFilter == null) {
        // documents are unchanged
        this.flrContext.maxDoc = this.in.maxDoc();
      } else {
        // find max value
        final int maxBit = this.flrContext.docBits.length() - 1;
        if (this.flrContext.docBits.get(maxBit)) {
          // highest bit is set
          this.flrContext.maxDoc = maxBit + 1;
        } else {
          // get the first bit set starting from the highest one
          final int maxDoc = this.flrContext.docBits.prevSetBit(maxBit);
          if (maxDoc >= 0) {
            this.flrContext.maxDoc = maxDoc + 1;
          } else {
            this.flrContext.maxDoc = 1;
          }
        }
      }

      // number of documents enabled after filtering
      this.flrContext.numDocs = this.flrContext.docBits.cardinality();

      // check, if still all documents are live
      if (this.flrContext.numDocs == this.flrContext.maxDoc) {
        this.flrContext.docBits = null; // all live
      }

      if (LOG.isDebugEnabled()) {
        final Collection<String> fields =
            new ArrayList<>(this.fieldInfos.size());
        for (final FieldInfo fi : this.fieldInfos) {
          fields.add(fi.name);
        }
        LOG.debug("Final state: numDocs={} maxDoc={} fields={}",
            this.flrContext.numDocs, this.flrContext.maxDoc, fields);
      }
      tFilter.setFlrContext(this.flrContext);
      this.flrContext.termFilter = tFilter;
      this.flrContext.context = this.getContext();
      this.flrContext.originContext = this.in.getContext();

      this.fieldsInstance = new FilteredFields(this.flrContext,
          this.in.fields(), this.fields);
    }

    /**
     * @param vFields Collection of fields visible to the reader
     * @throws IOException Thrown on low-level I/O-errors
     */
    private void applyFieldFilter(final Collection<String> vFields)
        throws IOException {
      final List<FieldInfo> filteredInfos = new ArrayList<>(vFields.size());

      for (final FieldInfo fi : this.in.getFieldInfos()) {
        if (hasField(fi.name)) {
          filteredInfos.add(fi);
        }
      }
      this.fieldInfos = new FieldInfos(filteredInfos.toArray(new
          FieldInfo[filteredInfos.size()]));

      // fields are now filtered, now enable documents only that have any
      // of the remaining fields

      // Bit-set indicating valid documents (after filtering).
      // A document whose bit is on is valid.
      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());
      for (final FieldInfo fi : this.fieldInfos) {
        @SuppressWarnings("ObjectAllocationInLoop")
        Filter f = this.flrContext.cachedFieldValueFilters.get(fi.name);
        if (f == null) {
          f = new CachingWrapperFilter(new EmptyFieldFilter(fi.name));
          this.flrContext.cachedFieldValueFilters.put(fi.name, f);
        }
        final DocIdSet docsWithField = f.getDocIdSet(
            this.in.getContext(), null); // accept all docs, no deletions

        // may be null, if no document matches
        if (docsWithField == null) {
          // remove field from list
          this.fields.remove(fi.name);
        } else {
          final DocIdSetIterator docsWithFieldIt = docsWithField.iterator();
          // may also be null, if no document matches
          if (docsWithFieldIt != null) {
            while (true) {
              final int docId = docsWithFieldIt.nextDoc();
              if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              filterBits.set(docId);
            }
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        if (this.flrContext.docBits != null) {
          LOG.debug("Filter (fields): {} -> {}",
              this.flrContext.docBits.cardinality(), filterBits.cardinality());
        } else {
          LOG.debug("Filter (fields): {} -> {}",
              this.flrContext.maxDoc, filterBits.cardinality());
        }
      }
      this.flrContext.docBits = filterBits;
    }

    /**
     * @param aFilter Filter to select documents provided by this reader
     * @throws IOException Thrown on low-level I/O errors
     */
    private void applyDocFilter(final Filter aFilter)
        throws IOException {
      final DocIdSetIterator keepDocs = aFilter.getDocIdSet(
          this.in.getContext(), this.flrContext.docBits).iterator();
      // Bit-set indicating valid documents (after filtering).
      // A document whose bit is on is valid.
      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());

      if (keepDocs != null) {
        // re-enable only those documents allowed by the filter
        while (true) {
          final int docId = keepDocs.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          // turn bit on, document is valid
          filterBits.set(docId);
        }
      }

      if (LOG.isDebugEnabled()) {
        if (this.flrContext.docBits != null) {
          LOG.debug("Filter (doc): {} -> {}",
              this.flrContext.docBits.cardinality(), filterBits.cardinality());
        } else {
          LOG.debug("Filter (doc): {} -> {}",
              this.flrContext.maxDoc, filterBits.cardinality());
        }
      }
      this.flrContext.docBits = filterBits;
    }

    /**
     * Check if a named field is valid (visible or filtered out)
     *
     * @param field Field name to check
     * @return True, if valid (visible), false otherwise
     */
    private boolean hasField(final String field) {
      return this.fields.isEmpty() ||
          this.negateFields ^ this.fields.contains(field);
    }

    @Override
    @Nullable
    public FixedBitSet getLiveDocs() {
      return this.flrContext.docBits;
    }

    @Override
    public FieldInfos getFieldInfos() {
      return this.fieldInfos;
    }

    @Override
    @Nullable
    public Fields getTermVectors(final int docID)
        throws IOException {
      Fields f = this.in.getTermVectors(docID);
      if (f == null) {
        return null;
      }
      f = new FilteredFields(this.flrContext, f, this.fields);
      return f.iterator().hasNext() ? f : null;
    }

    @Override
    public int numDocs() {
      return this.flrContext.numDocs;
    }

    @Override
    public int maxDoc() {
      return this.flrContext.maxDoc;
    }

    @Override
    // taken from FieldFilterAtomicReader
    public void document(final int docID, final StoredFieldVisitor visitor)
        throws IOException {
      if (isFieldsFiltered()) {
        this.in.document(docID, new StoredFieldVisitor() {
          @Override
          public void binaryField(final FieldInfo fieldInfo, final byte[] value)
              throws IOException {
            visitor.binaryField(fieldInfo, value);
          }

          @Override
          public void stringField(final FieldInfo fieldInfo, final String value)
              throws IOException {
            visitor.stringField(fieldInfo, value);
          }

          @Override
          public void intField(final FieldInfo fieldInfo, final int value)
              throws IOException {
            visitor.intField(fieldInfo, value);
          }

          @Override
          public void longField(final FieldInfo fieldInfo, final long value)
              throws IOException {
            visitor.longField(fieldInfo, value);
          }

          @Override
          public void floatField(final FieldInfo fieldInfo, final float value)
              throws IOException {
            visitor.floatField(fieldInfo, value);
          }

          @Override
          public void doubleField(final FieldInfo fieldInfo, final double value)
              throws IOException {
            visitor.doubleField(fieldInfo, value);
          }

          @Override
          public Status needsField(final FieldInfo fieldInfo)
              throws IOException {
            return hasField(fieldInfo.name) ? visitor.needsField(fieldInfo) :
                Status.NO;
          }
        });
      } else {
        // no fields filtered - use plain instance
        this.in.document(docID, visitor);
      }
    }

    /**
     * Check if fields visible to the reader are filtered. This gets used to
     * avoid some extra processing if not really needed.
     *
     * @return True, if filtered
     */
    private boolean isFieldsFiltered() {
      return !this.fields.isEmpty();
    }

    @Override
    public Fields fields() {
      return this.fieldsInstance;
    }

    @Override
    @Nullable
    public NumericDocValues getNumericDocValues(final String field)
        throws IOException {
      return hasField(field) ? super.getNumericDocValues(field) : null;
    }

    @Override
    @Nullable
    public BinaryDocValues getBinaryDocValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getBinaryDocValues(field) : null;
    }

    @Override
    @Nullable
    public SortedDocValues getSortedDocValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getSortedDocValues(field) : null;
    }

    @Override
    @Nullable
    public SortedNumericDocValues getSortedNumericDocValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getSortedNumericDocValues(field) : null;
    }

    @Override
    @Nullable
    public SortedSetDocValues getSortedSetDocValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getSortedSetDocValues(field) : null;
    }

    @Override
    @Nullable
    public NumericDocValues getNormValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getNormValues(field) : null;
    }

    @Override
    @Nullable
    public final FixedBitSet getDocsWithField(final String field)
        throws IOException {
      if (!hasField(field)) {
        return null;
      }

      // get a bit-set of matching docs from the original reader..
      // AND them with the allowed documents to get only visible matches
      final FixedBitSet filteredDocs = BitsUtils.Bits2FixedBitSet(
          this.in.getDocsWithField(field));
      if (filteredDocs == null) {
        return null;
      }
      if (this.flrContext.docBits != null) {
        filteredDocs.and(this.flrContext.docBits);
      }
      return filteredDocs;
    }

    /**
     * Contextual information for an FilteredLeafReader instance.
     */
    static class FLRContext {
      /**
       * Store cached results of {@link FieldValueFilter}s.
       */
      final Map<String, Filter> cachedFieldValueFilters;
      /**
       * Bits with visible documents bits turned on.
       */
      @Nullable
      @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
      FixedBitSet docBits;
      TermFilter termFilter;
      LeafReaderContext context;
      LeafReaderContext originContext;
      /**
       * Number of visible documents.
       */
      int numDocs;
      /**
       * Highest document number.
       */
      int maxDoc;

      FLRContext() {
        this.cachedFieldValueFilters = Collections.synchronizedMap(
            new HashMap<>(15));
      }
    }

    /**
     * Wrapper for a {@link Fields} instance providing filtering.
     */
    static class FilteredFields
        extends FilterFields {
      /**
       * Fields collected from all visible documents.
       */
      private final List<String> fields;
      /**
       * Caches total document/term frequency values from any {@link
       * FilteredTerms} instance.
       */
      private final Map<String, Long[]> fieldTermsSumCache;
      private final FLRContext flr;

      /**
       * Creates a new FilterFields.
       *
       * @param originFields Original Fields instance
       */
      FilteredFields(final FLRContext flr,
          final Fields originFields, final Collection<String> fields) {
        super(originFields);
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields t={}",
              Thread.currentThread().getName());
        }
        this.flr = flr;
        this.fieldTermsSumCache = Collections.synchronizedMap(
            new HashMap<>(fields.size() * 2));
        // collect visible document fields
        this.fields = Collections.unmodifiableList(getFields(fields));
      }

      /**
       * Get a list of all visible document fields.
       *
       * @return List of available document fields
       */
      private List<String> getFields(final Collection<String> fields) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields::getFields() t={}",
              Thread.currentThread().getName());
        }
        return StreamSupport.stream(this.in.spliterator(), false)
            .filter(f -> {
              try {
                return fields.contains(f) &&
                    new FilteredTerms(this.flr, this,
                        this.in.terms(f), f).hasDoc();
              } catch (final IOException e) {
                LOG.error("Error parsing terms in field '{}'.", f, e);
                throw new UncheckedIOException(e);
              }
            })
            .collect(Collectors.toList());
      }

      @Override
      public Iterator<String> iterator() {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields::iterator() t={}",
              Thread.currentThread().getName());
        }
        return this.fields.iterator();
      }

      @Override
      @Nullable
      public Terms terms(final String field)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields::terms() t={}",
              Thread.currentThread().getName());
        }
        if (this.fields.contains(field)) {
          return new FilteredTerms(this.flr, this, this.in.terms(field), field);
        } else {
          return null;
        }
      }

      @Override
      public int size() {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields::size() t={}",
              Thread.currentThread().getName());
        }
        return this.fields.size();
      }

      /**
       * Returns the original Terms instance from the wrapped reader.
       *
       * @param field Field name
       * @return original Terms instance from the wrapped reader
       * @throws IOException Thrown on low-level I/O-errors
       */
      @Nullable
      public Terms originalTerms(final String field)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredFields::originalTerms() t={}",
              Thread.currentThread().getName());
        }
        return this.in.terms(field);
      }
    }

    /**
     * Wrapper for a {@link Terms} instance providing filtering.
     */
    static class FilteredTerms
        extends FilterTerms {
      /**
       * Current field name.
       */
      private final String field;
      /**
       * Cached values of total document/term frequency. Set once it's
       * calculated, otherwise the value is -1.
       */
      private final Long[] sumFreqs;
      private final FLRContext flr;

      /**
       * Creates a new FilterTerms, passing a FilterFields instance. This gets
       * used, if the global instance has not yet initialized.
       *
       * @param originTerms the underlying Terms instance
       * @param aField Current field.
       * @param ffInstance FilteredFields instance, if not initialized already
       */
      FilteredTerms(final FLRContext flr,
          final FilteredFields ffInstance,
          final Terms originTerms, final String aField) {
        super(originTerms);
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::new t={}",
              Thread.currentThread().getName());
        }
        this.flr = flr;
        this.field = aField;

        // initialized cached frequency values
        Long[] sumFreqs = ffInstance.fieldTermsSumCache.get(this.field);
        if (sumFreqs == null) {
          sumFreqs = new Long[]{-1L, -1L};
          ffInstance.fieldTermsSumCache.put(this.field, sumFreqs);
        }
        this.sumFreqs = sumFreqs;
      }

      /**
       * Checks, if a document with the current field is visible.
       *
       * @return True if any document contains any term in the current field
       * @throws IOException Thrown on low-level I/O errors
       */
      public boolean hasDoc()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::hasDoc() t={}",
              Thread.currentThread().getName());
        }
        final TermsEnum termsEnum = iterator(null);
        DocsEnum docsEnum = null;
        while (true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }

          docsEnum = termsEnum.docs(this.flr.docBits,
              docsEnum, DocsEnum.FLAG_NONE);
          if (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            return true;
          }
        }
        return false;
      }

      @Override
      public TermsEnum iterator(@Nullable final TermsEnum reuse)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::iterator() t={}",
              Thread.currentThread().getName());
        }
        return new FilteredTermsEnum(this.flr, this.in.iterator(reuse));
      }

      @Override
      public long size()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::size() t={}",
              Thread.currentThread().getName());
        }
        // TODO: implement & calc at startup
        final RuntimeException e = new UnsupportedOperationException();
        LOG.error("Not implemented", e);
        throw e;
      }

      @Override
      public synchronized long getSumTotalTermFreq()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::getSumTotalTermFreq() t={}",
              Thread.currentThread().getName());
        }

        if (this.sumFreqs[1] < 0L) {
          final TermsEnum te = iterator(null);
          this.sumFreqs[1] = StreamSupport.longStream(new OfLong() {
            @Override
            @Nullable
            public OfLong trySplit() {
              return null; // no split support
            }

            @Override
            public long estimateSize() {
              return Long.MAX_VALUE; // we don't know
            }

            @Override
            public boolean tryAdvance(final LongConsumer action) {
              try {
                final BytesRef nextTerm = te.next();
                if (nextTerm == null) {
                  return false;
                } else {
                  if (FilteredTerms.this.flr.termFilter.accept(te, nextTerm)) {
                    action.accept(te.totalTermFreq());
                  }
                  return true;
                }
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }

            @Override
            public int characteristics() {
              return IMMUTABLE; // not mutable
            }
          }, false).sum();
        }
        return this.sumFreqs[1];
      }

      @Override
      public synchronized long getSumDocFreq()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::getSumDocFreq() t={}",
              Thread.currentThread().getName());
        }

        if (this.sumFreqs[0] < 0L) {
          final TermsEnum te = iterator(null);
          this.sumFreqs[0] = StreamSupport.longStream(new OfLong() {
            @Override
            public boolean tryAdvance(final LongConsumer action) {
              try {
                final BytesRef nextTerm = te.next();
                if (nextTerm == null) {
                  return false;
                } else {
                  if (FilteredTerms.this.flr.termFilter.accept(te, nextTerm)) {
                    action.accept((long) te.docFreq());
                  }
                  return true;
                }
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }

            @Override
            @Nullable
            public OfLong trySplit() {
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
          }, false).sum();
        }
        return this.sumFreqs[0];
      }


      @Override
      public int getDocCount()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::getDocCount() t={}",
              Thread.currentThread().getName());
        }
        Filter f = this.flr.cachedFieldValueFilters.get(this.field);

        if (f == null) {
          f = new CachingWrapperFilter(new FieldValueFilter(this.field));
          this.flr.cachedFieldValueFilters.put(this.field, f);
        }

        final DocIdSet docsWithField = f.getDocIdSet(
            this.flr.originContext, this.flr.docBits);

        if (docsWithField == null) {
          return 0;
        }

        final DocIdSetIterator docsWithFieldIt = docsWithField.iterator();
        if (docsWithFieldIt == null) {
          return 0;
        }

        int count = 0;
        while (true) {
          final int docId = docsWithFieldIt.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          count++;
        }
        return count;
      }

      /**
       * Get the original TermsEnum instance from the wrapped reader.
       *
       * @param reuse TermsEnum for reuse
       * @return TermsEnum instance received from wrapped reader
       * @throws IOException Thrown on low-level I/O-errors
       */
      public TermsEnum unfilteredIterator(@Nullable final TermsEnum reuse)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTerms::unfilteredIterator() t={}",
              Thread.currentThread().getName());
        }
        return this.in.iterator(reuse);
      }
    }

    @Override
    public boolean hasDeletions() {
      // for sure, because we don't support indices with deleted documents
      return false;
    }

    /**
     * Wrapper for a {@link TermsEnum} instance providing filtering.
     */
    static class FilteredTermsEnum
        extends FilterTermsEnum {
      private final FLRContext flr;
      /**
       * Shared {@link DocsEnum} instance.
       */
      @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
      private DocsEnum sharedDocsEnum;
      /**
       * On-time retrieval result of document-frequency and total-term-frequency
       * value.
       */
      private long[] freqs;

      /**
       * Creates a new FilterTermsEnum.
       *
       * @param originTe the underlying TermsEnum instance
       */
      FilteredTermsEnum(final FLRContext flr, final TermsEnum originTe) {
        super(originTe);
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::new t={}",
              Thread.currentThread().getName());
        }
        this.flr = flr;
      }

      @Override
      public SeekStatus seekCeil(final BytesRef term)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::seekCeil t={}",
              Thread.currentThread().getName());
        }
        // try seek to term
        final SeekStatus status = this.in.seekCeil(term);

        // check, if we hit the end of the term list
        // or term is contained in any visible document
        if (status == SeekStatus.END || hasDoc()) {
          return status;
        }

        // get next term, document visibility checked in next() method
        if (next() == null) {
          return SeekStatus.END;
        }
        return SeekStatus.NOT_FOUND;
      }

      /**
       * Check, if the current term is contained in any visible document.
       *
       * @return True, if any visible document contains this term
       * @throws IOException thrown on low-level I/O-errors
       */
      private boolean hasDoc()
          throws IOException {
        // check, if term is contained in any visible document
        return DocIdSetIterator.NO_MORE_DOCS != this.in
            .docs(this.flr.docBits,
                this.sharedDocsEnum, DocsEnum.FLAG_NONE).nextDoc();
      }

      /**
       * Returns the next term, if any, excluding terms not currently in the
       * visible documents.
       *
       * @return Next term or {@code null}, if there's none left
       * @throws IOException Thrown on low-level I/O-errors
       */
      @Override
      @Nullable
      public BytesRef next()
          throws IOException {

        while (true) {
          final BytesRef term = this.in.next();
          if (term == null) {
            return null;
          }
          // check, if term is contained in any visible document
          if (hasDoc() && this.flr.termFilter.accept(this.in, term)) {
            return term;
          }
        }
      }

      @Override
      public int docFreq()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::docFreq() t={}",
              Thread.currentThread().getName());
        }
        return (int) (this.freqs == null ? freqs()[0] : this.freqs[0]);
      }

      /**
       * Get the document-frequency and the total-term-frequency value at the
       * same time.
       *
       * @return Array [docFreq, TTF]
       * @throws IOException Thrown on low-level I/O-errors
       */
      private synchronized long[] freqs()
          throws IOException {
        if (this.freqs == null) {
          this.sharedDocsEnum = this.in.docs(this.flr.docBits,
              this.sharedDocsEnum, DocsEnum.FLAG_FREQS);
          final long[] freqs = {0L, 0L}; // docFreq, ttf
          while (this.sharedDocsEnum.nextDoc() !=
              DocIdSetIterator.NO_MORE_DOCS) {
            freqs[0]++; // docFreq
            freqs[1] += (long) this.sharedDocsEnum.freq(); // ttf
          }
          this.freqs = freqs;
        }
        return this.freqs;
      }

      @Override
      public long totalTermFreq()
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::totalTermFreq() t={}",
              Thread.currentThread().getName());
        }
        return this.freqs == null ? freqs()[1] : this.freqs[1];
      }

      @Override
      public DocsEnum docs(@Nullable final Bits liveDocs,
          @Nullable final DocsEnum reuse, final int flags)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::docs() t={}",
              Thread.currentThread().getName());
        }
        if (liveDocs == null) {
          return this.in.docs(this.flr.docBits, reuse, flags);
        } else {
          if (this.flr.docBits == null) {
            return this.in.docs(liveDocs, reuse, flags);
          }
          final FixedBitSet liveBits = this.flr.docBits.clone();
          liveBits.and(BitsUtils.Bits2FixedBitSet(liveDocs));
          return this.in.docs(liveBits, reuse, flags);
        }
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(
          @Nullable final Bits liveDocs,
          @Nullable final DocsAndPositionsEnum reuse, final int flags)
          throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("@FilteredTermsEnum::docsAndPositions() t={}",
              Thread.currentThread().getName());
        }
        if (liveDocs == null) {
          return this.in.docsAndPositions(this.flr.docBits, reuse, flags);
        } else {
          if (this.flr.docBits == null) {
            return this.in.docsAndPositions(liveDocs, reuse, flags);
          }
          final FixedBitSet liveBits = this.flr.docBits.clone();
          liveBits.and(BitsUtils.Bits2FixedBitSet(liveDocs));
          return this.in.docsAndPositions(liveBits, reuse, flags);
        }
      }
    }
  }

  public static final class Builder {
    private final DirectoryReader dr;
    private Filter qf;
    private TermFilter tf;
    private boolean fn;
    private Collection<String> f;

    public Builder(final DirectoryReader dirReader) {
      this.dr = dirReader;
    }

    public Builder queryFilter(final Filter qFilter) {
      this.qf = qFilter;
      return this;
    }

    public Builder fields(
        final Collection<String> fields) {
      return fields(fields, false);
    }

    public Builder fields(
        final Collection<String> fields, final boolean negate) {
      this.f = fields;
      this.fn = negate;
      return this;
    }

    public Builder termFilter(final TermFilter tFilter) {
      this.tf = tFilter;
      return this;
    }

    public FilteredDirectoryReader build() {
      if (this.tf == null) {
        this.tf = new AcceptAllTermFilter();
      }
      final SubReaderWrapper srw = new SubReaderWrapper() {
        @Override
        public LeafReader wrap(final LeafReader reader) {
          try {
            return new FilteredLeafReader(reader, Builder.this.f,
                Builder.this.fn, Builder.this.qf, Builder.this.tf);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };
      return new FilteredDirectoryReader(
          this.dr, srw, this.f, this.fn, this.qf, this.tf);
    }
  }

  /**
   * Default term-filter accepting all terms.
   */
  private static final class AcceptAllTermFilter
      extends TermFilter {

    @Override
    public boolean accept(final TermsEnum termsEnum, final BytesRef term) {
      return true;
    }
  }

  public static abstract class TermFilter {
    @Nullable
    protected FilteredDirectoryReader topReader;
    @Nullable
    protected FilteredLeafReader.FLRContext flrContext;

    /**
     * @param termsEnum TermsEnum currently in use. Be careful not to change the
     * current position of the enum while filtering.
     * @param term Current term
     * @return AcceptStatus indicating, if term is valid (should be returned)
     * @throws IOException Thrown on low-level I/O-errors
     */
    public abstract boolean accept(
        @Nullable final TermsEnum termsEnum, final BytesRef term)
        throws IOException;

    protected void setTopReader(final FilteredDirectoryReader reader) {
      this.topReader = reader;
    }

    protected void setFlrContext(
        final FilteredLeafReader.FLRContext context) {
      this.flrContext = context;
    }

    /**
     * Filter based on a list of stopwords wrapping another filter.
     */
    public static final class StopwordWrapper
        extends TermFilter {
      /**
       * Wrapped filter.
       */
      private final TermFilter in;
      private final BytesRefHash sWords;

      public StopwordWrapper(
          final Collection<String> sWords, final TermFilter in) {
        this.in = in;
        this.sWords = new BytesRefHash();
        for (final String sw : sWords) {
          this.sWords.add(new BytesRef(sw));
        }
      }

      @Override
      public boolean accept(
          @Nullable final TermsEnum termsEnum, final BytesRef term)
          throws IOException {
        if (this.sWords.find(term) > -1) {
          return false;
        }
        return this.in.accept(termsEnum, term);
      }
    }

    /**
     * Common terms term-filter. Skips terms exceeding a defined document
     * frequency threshold.
     */
    public static final class CommonTerms
        extends TermFilter {
      /**
       * Common terms collected so flr.
       */
      private final BytesRefHash commonTerms = new BytesRefHash();
      /**
       * Document frequency threshold.
       */
      private final double t;
      /**
       * Number of documents available in the index.
       */
      private int docCount = -1;
      /**
       * Number of documents available in the index.
       */
      private double docCountDiv;
      /**
       * Array of sub-readers from top-level having at least one document.
       */
      private LeafReader[] subReaders;
      /**
       * Number of sub-readers used by top-level.
       */
      private int subReaderCount;
      /**
       * Bits set for documents to check.
       */
      private FixedBitSet checkBits;
      private int limit;

      public CommonTerms(final double threshold) {
        this.t = threshold;
      }

      void countDocs() {
        assert this.topReader != null;
        this.docCount = this.topReader.fields.stream()
            .mapToInt(f -> {
              try {
                return this.topReader.unwrap().getDocCount(f);
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            }).sum();
        assert this.docCount > 0;
        this.docCountDiv = 1.0 / this.docCount;
        this.limit = (int) Math.floor((double) this.docCount * this.t);
      }

      /**
       * Checks, if the current frequency value will be accepted by the current
       * threshold value.
       *
       * @param docFreq Frequency value
       * @return True, if accepted, false otherwise
       */
      private boolean isAccepted(final int docFreq) {
        return ((double) docFreq * this.docCountDiv) <= this.t;
      }

      @Override
      protected void setTopReader(final FilteredDirectoryReader reader) {
        super.setTopReader(reader);
        assert this.topReader != null;
        this.subReaders = this.topReader.getSequentialSubReaders().stream()
            // skip readers without documents
            .filter(r -> r.numDocs() > 0)
            .toArray(LeafReader[]::new);
        this.subReaderCount = this.subReaders.length;
        this.checkBits = BitsUtils.Bits2FixedBitSet(
            MultiFields.getLiveDocs(this.topReader));
        if (this.checkBits == null) {
          // all documents are live
          this.checkBits = new FixedBitSet(this.topReader.maxDoc());
          this.checkBits.set(0, this.checkBits.length());
        }
        countDocs();
      }

      @Override
      public boolean accept(
          @Nullable final TermsEnum termsEnum, final BytesRef term)
          throws IOException {
        if (this.topReader == null) {
          // pass through all terms at initialization time
          return true;
        }

        if (this.commonTerms.find(term) > -1) {
          return false;
        }

        DocsEnum de = null;
        TermsEnum te = null;
        final FixedBitSet hitBits = new FixedBitSet(this.checkBits.length());
        final FixedBitSet checkBits = this.checkBits.clone();
        int count = this.limit;

        for (int i = this.subReaderCount - 1; i >= 0; i--) {
          final FilteredLeafReader.FilteredFields ffields =
              (FilteredLeafReader.FilteredFields)
                  this.subReaders[i].fields();
          final int fieldCount = ffields.fields.size();
          for (int j = fieldCount - 1; j >= 0; j--) {
            final Terms t = ffields.originalTerms(ffields.fields.get(j));

            if (t != null) {
              te = t.iterator(te);

              if (te.seekExact(term)) {
                // check, if threshold is exceeded
                if (!isAccepted(te.docFreq())) {
                  this.commonTerms.add(term);
                  return false;
                }

                de = te.docs(checkBits, de);

                int docId;
                while ((docId = de.nextDoc()) !=
                    DocIdSetIterator.NO_MORE_DOCS) {
                  if (!hitBits.getAndSet(docId)) {
                    // new doc
                    checkBits.clear(docId);
                    count--;
                  }
                  if (count == 0 &&
                      !isAccepted(hitBits.cardinality())) {
                    this.commonTerms.add(term);
                    return false;
                  }
                }
              }
            }
          }
        }
        // check, if threshold is exceeded
        if (isAccepted(hitBits.cardinality())) {
          return true;
        }

        this.commonTerms.add(term);
        return false;
      }
    }
  }
}
