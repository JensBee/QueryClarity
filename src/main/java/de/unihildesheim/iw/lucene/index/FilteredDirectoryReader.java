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

import de.unihildesheim.iw.lucene.index.TermFilter.AcceptAll;
import de.unihildesheim.iw.lucene.search.EmptyFieldFilter;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public final class FilteredDirectoryReader
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
      if (!AcceptAll.class.isInstance(tFilter)) {
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fields: {}", this.fields);
    }

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
  protected DirectoryReader doWrapDirectoryReader(
      final DirectoryReader dirReader) {
    return new FilteredDirectoryReader(dirReader, this.subWrapper,
        this.fields, this.negateFields, this.filter, this.termFilter);
  }

  @Override
  public String toString() {
    return "FilteredDirectoryReader: " + super.toString();
  }

  @Override
  public boolean hasDeletions() {
    // for sure, because we don't support indices with deleted documents
    return false;
  }

  /**
   * Get the currently visible fields.
   *
   * @return Visible fields collected from all sub-readers
   */
  @SuppressWarnings("TypeMayBeWeakened")
  public Set<String> getFields() {
    return Collections.unmodifiableSet(this.fields);
  }

  /**
   * Get all sub-readers associated with this composite reader.
   *
   * @return Sub-readers
   */
  @SuppressWarnings("unchecked")
  public Collection<FilteredLeafReader> getSubReaders() {
    return Collections.unmodifiableList((List<FilteredLeafReader>) this
        .getSequentialSubReaders());
  }

  /**
   * Filtered {@link LeafReader} that wraps another AtomicReader and provides
   * filtering functions.
   */
  public static final class FilteredLeafReader
      extends LeafReader {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(FilteredLeafReader.class);
    /**
     * Constant value if no fields are selected.
     */
    private static final String[] NO_FIELDS = new String[0];
    /**
     * Fields instance.
     */
    private final FilteredFields fieldsInstance;
    /**
     * Set of field names visible.
     */
    private final String[] fields;
    /**
     * If true, given fields should be negated.
     */
    private final boolean negateFields;
    /**
     * Contextual meta-data.
     */
    private final FLRContext flrContext;
    /**
     * {@link FieldInfos} reduced to the visible fields.
     */
    private final FieldInfos fieldInfos;
    /**
     * The underlying LeafReader.
     */
    private final LeafReader in;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterAtomicReader is
     * closed.</p>
     *
     * @param wrap specified base reader to wrap
     * @param vFields Collection of fields visible to the reader
     * @param negate If true, given fields should be negated
     * @param qFilter Filter to reduce the number of documents visible to this
     * reader
     * @param tFilter Term-filter
     * @throws IOException Thrown on low-level I/O-errors
     */
    FilteredLeafReader(final LeafReader wrap,
        final Collection<String> vFields, final boolean negate,
        @Nullable final Filter qFilter, final TermFilter tFilter)
        throws IOException {
      this.in = Objects.requireNonNull(wrap);

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
        this.fields = NO_FIELDS;
      } else {
        // fields are now filtered, now enable documents only that have any
        // of the remaining fields
        this.fields = applyFieldFilter(vFields, negate);
        this.fieldInfos = new FieldInfos(StreamSupport.stream(
            this.in.getFieldInfos().spliterator(), false)
            .filter(fi -> hasField(fi.name))
            .toArray(FieldInfo[]::new));
      }

      if (qFilter != null) {
        // user document filter gets applied
        applyDocFilter(qFilter);
      }

      // set maxDoc value
      if (this.fields.length == 0 && qFilter == null) {
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
          this.flrContext.maxDoc = maxDoc >= 0 ? maxDoc + 1 : 1;
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

      // create context
      this.flrContext.termFilter = tFilter;
      this.flrContext.context = this.getContext();
      this.flrContext.originContext = this.in.getContext();

      this.fieldsInstance = new FilteredFields(this.flrContext,
          this.in.fields(), this.fields);
    }

    @Override
    public String toString() {
      return "FilteredFields [" + this.in + ']';
    }

    /**
     * @param vFields Collection of fields visible to the reader
     * @param negate If true, given fields should be negated
     * @return Fields with fields without documents removed
     * @throws IOException Thrown on low-level I/O-errors
     */
    @SuppressWarnings("ObjectAllocationInLoop")
    private String[] applyFieldFilter(
        final Collection<String> vFields, final boolean negate)
        throws IOException {

      final String[] fields = StreamSupport.stream(
          this.in.fields().spliterator(), false)
          .filter(f -> negate ^ vFields.contains(f))
          .toArray(String[]::new);
      final List<String> finalFields = new ArrayList<>(this.in.fields().size());

      // Bit-set indicating valid documents (after filtering).
      // A document whose bit is on is valid.
      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());
      for (final String field : fields) {
        Filter f = this.flrContext.cachedFieldValueFilters.get(field);
        if (f == null) {
          f = new CachingWrapperFilter(new EmptyFieldFilter(field));
          this.flrContext.cachedFieldValueFilters.put(field, f);
        }
        final DocIdSet docsWithField = f.getDocIdSet(
            this.in.getContext(), null); // isAccepted all docs, no deletions

        // may be null, if no document matches
        if (docsWithField != null) {
          finalFields.add(field);
          StreamUtils.stream(docsWithField).forEach(filterBits::set);
//          final DocIdSetIterator docsWithFieldIt = docsWithField.iterator();
//          // may also be null, if no document matches
//          if (docsWithFieldIt != null) {
//            int docId;
//            while ((docId = docsWithFieldIt.nextDoc()) !=
//                DocIdSetIterator.NO_MORE_DOCS) {
//              filterBits.set(docId);
//            }
//          }
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

      return finalFields.isEmpty() ? NO_FIELDS :
          finalFields.toArray(new String[finalFields.size()]);
    }

    /**
     * Check if a named field is valid (visible or filtered out)
     *
     * @param field Field name to check
     * @return True, if valid (visible), false otherwise
     */
    boolean hasField(final String field) {
      return this.fields.length == 0 ||
          //this.negateFields ^ this.fields.contains(field);
          this.negateFields ^ (Arrays.binarySearch(this.fields, field) >= 0);
    }

    /**
     * @param aFilter Filter to select documents provided by this reader
     * @throws IOException Thrown on low-level I/O errors
     */
    private void applyDocFilter(final Filter aFilter)
        throws IOException {

      // Bit-set indicating valid documents (after filtering).
      // A document whose bit is on is valid.
      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());
      StreamUtils.stream(aFilter.getDocIdSet(
          this.in.getContext(), this.flrContext.docBits))
          .forEach(filterBits::set);

//      final DocIdSetIterator keepDocs = aFilter.getDocIdSet(
//          this.in.getContext(), this.flrContext.docBits).iterator();
//      // Bit-set indicating valid documents (after filtering).
//      // A document whose bit is on is valid.
//      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());
//
//      if (keepDocs != null) {
//        // re-enable only those documents allowed by the filter
//        int docId;
//        while ((docId = keepDocs.nextDoc()) != DocIdSetIterator
// .NO_MORE_DOCS) {
//          // turn bit on, document is valid
//          filterBits.set(docId);
//        }
//      }

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

    @Override
    public void addCoreClosedListener(final CoreClosedListener listener) {
      this.in.addCoreClosedListener(listener);
    }

    @Override
    public void removeCoreClosedListener(final CoreClosedListener listener) {
      this.in.removeCoreClosedListener(listener);
    }

    @Override
    public Fields fields() {
      return this.fieldsInstance;
    }

    @Override
    @Nullable
    public NumericDocValues getNumericDocValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getNumericDocValues(field) : null;
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
    public BitSet getDocsWithField(final String field)
        throws IOException {
      if (!hasField(field)) {
        return null;
      }

      // get a bit-set of matching docs from the original reader..
      // AND them with the allowed documents to get only visible matches
      final BitSet filteredDocs;
      if (this.flrContext.docBits == null) {
        filteredDocs = BitsUtils.bits2BitSet(this.in.getDocsWithField(field));
        if (filteredDocs == null) {
          return null;
        }
      } else {
        filteredDocs = BitsUtils.bits2FixedBitSet(
            this.in.getDocsWithField(field));
        if (filteredDocs == null) {
          return null;
        }
        ((FixedBitSet)filteredDocs).and(this.flrContext.docBits);
      }
//      final FixedBitSet filteredDocs = BitsUtils.bits2FixedBitSet(
//          this.in.getDocsWithField(field));
//      if (filteredDocs == null) {
//        return null;
//      }
//      if (this.flrContext.docBits != null) {
//        filteredDocs.and(this.flrContext.docBits);
//      }
      return filteredDocs;
    }

    @Override
    @Nullable
    public NumericDocValues getNormValues(final String field)
        throws IOException {
      return hasField(field) ? this.in.getNormValues(field) : null;
    }

    @Override
    protected void doClose()
        throws IOException {
      this.in.close();
    }

    @Override
    public FieldInfos getFieldInfos() {
      return this.fieldInfos;
    }

    @Override
    @Nullable
    public BitSet getLiveDocs() {
      return this.flrContext.docBits;
    }

    @Override
    public void checkIntegrity()
        throws IOException {
      ensureOpen();
      this.in.checkIntegrity();
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
      return this.fields.length != 0;
    }


    @Override
    public boolean hasDeletions() {
      // for sure, because we don't support indices with deleted documents
      return false;
    }
  }

  /**
   * Builder to create a new {@link FilteredDirectoryReader} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Builder.class);
    /**
     * Original DirectoryReader instance being wrapped.
     */
    final DirectoryReader in;
    /**
     * Query filter.
     */
    @Nullable
    Filter qf;
    /**
     * Term filter.
     */
    @Nullable
    TermFilter tf;
    /**
     * Flag indicating, if TermFilter should be negated.
     */
    boolean fn;
    /**
     * Fields visible.
     */
    @Nullable
    Collection<String> f;

    @Override
    public String toString() {
      return "Builder (FilteredDirectoryReader) " + super.toString();
    }

    /**
     * Creates a new builder wrapping a existing DirectoryReader.
     *
     * @param wrap Instance to wrap
     */
    public Builder(final DirectoryReader wrap) {
      this.in = wrap;
    }

    /**
     * Sets the query filter to reduce the number of visible documents.
     *
     * @param qFilter Filter instance
     * @return Self reference
     */
    public Builder queryFilter(final Filter qFilter) {
      this.qf = qFilter;
      return this;
    }

    /**
     * Sets the fields that should be visible. May be reduced later, if there
     * are no documents having a requested field set.
     *
     * @param vFields Fields that should be visible
     * @return Self reference
     */
    public Builder fields(
        final Collection<String> vFields) {
      this.f = new HashSet<>(vFields.size());
      this.f.addAll(vFields);
      this.fn = false;
      return this;
    }

    /**
     * Sets the fields that should be visible. May be reduced later, if there
     * are no documents having a requested field set.
     *
     * @param vFields Fields that should be visible
     * @param negate Inverts the list of fields. All passed in fields will be
     * hidden.
     * @return Self reference
     */
    @SuppressWarnings("BooleanParameter")
    public Builder fields(
        final Collection<String> vFields, final boolean negate) {
      this.f = new HashSet<>(vFields.size());
      this.f.addAll(vFields);
      this.fn = negate;
      return this;
    }

    /**
     * Sets the term-filter for reducing the visible terms.
     *
     * @param tFilter Filter instance.
     * @return Self reference
     */
    public Builder termFilter(final TermFilter tFilter) {
      this.tf = tFilter;
      return this;
    }

    /**
     * Creates the FilteredDirectoryReader instance.
     *
     * @return New FilteredDirectoryReader instance
     */
    public FilteredDirectoryReader build() {
      if (this.tf == null) {
        this.tf = new AcceptAll();
      }
      if (this.f == null) {
        this.f = Collections.emptySet();
      }

      final SubReaderWrapper srw = new SubReaderWrapper() {
        @Override
        public LeafReader wrap(final LeafReader reader) {
          try {
            assert Builder.this.f != null;
            assert Builder.this.tf != null;
            return new FilteredLeafReader(reader, Builder.this.f,
                Builder.this.fn, Builder.this.qf, Builder.this.tf);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };
      return new FilteredDirectoryReader(
          this.in, srw, this.f, this.fn, this.qf, this.tf);
    }
  }

  /**
   * Wrapper for a {@link Fields} instance providing filtering.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FilteredFields
      extends Fields {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(FilteredFields.class);
    /**
     * Fields collected from all visible documents.
     */
    private final String[] fields;
    /**
     * Caches total document/term frequency values from any FilteredTerms
     * instance.
     */
    private final Map<String, long[]> fieldTermsSumCache;
    /**
     * Context information for FilteredLeafReader.
     */
    private final FLRContext ctx;
    /**
     * The underlying Fields instance.
     */
    private final Fields in;

    /**
     * Creates a new FilterFields.
     *
     * @param flr Filtered reader context
     * @param wrap Original Fields instance
     * @param fld Visible fields
     */
    FilteredFields(
        final FLRContext flr, final Fields wrap, final String... fld) {
      this.in = wrap;
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredFields t={}", Thread.currentThread().getName());
      }
      this.ctx = flr;
      this.fieldTermsSumCache = Collections.synchronizedMap(
          new HashMap<>(fld.length << 1));
      // collect visible document fields
      this.fields = getFields(fld);
    }

    /**
     * Get a list of all visible document fields that have any content.
     *
     * @param fld Field initially set visible
     * @return List of available document fields
     */
    private String[] getFields(final String... fld) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredFields::getFields() t={}",
            Thread.currentThread().getName());
      }
      return StreamSupport.stream(this.in.spliterator(), false)
          .filter(f -> {
            try {
              return (Arrays.binarySearch(fld, f) >= 0) &&
                  new FilteredTerms(this.ctx, this,
                      this.in.terms(f), f).hasDoc();
            } catch (final IOException e) {
              LOG.error("Error parsing terms in field '{}'.", f, e);
              throw new UncheckedIOException(e);
            }
          })
          .sorted()
          .toArray(String[]::new);
    }

    /**
     * Get all visible fields.
     *
     * @return Visible fields
     */
    public String[] getFields() {
      return this.fields.clone();
    }

    @Override
    public String toString() {
      return "FilteredFields [" + this.in + ']';
    }

    @Override
    public Iterator<String> iterator() {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredFields::iterator() t={}",
            Thread.currentThread().getName());
      }
      return Arrays.stream(this.fields).iterator();
    }

    @Override
    @Nullable
    public Terms terms(final String field)
        throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredFields::terms() t={}",
            Thread.currentThread().getName());
      }
      return Arrays.binarySearch(this.fields, field) >= 0 ?
          new FilteredTerms(this.ctx, this, this.in.terms(field), field) : null;
    }

    @Override
    public int size() {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredFields::size() t={}",
            Thread.currentThread().getName());
      }
      return this.fields.length;
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
   * Contextual information for an FilteredLeafReader instance.
   */
  @SuppressWarnings({"PackageVisibleInnerClass", "PackageVisibleField"})
  static final class FLRContext {
    /**
     * Store cached results of {@link FieldValueFilter}s.
     */
    final Map<String, Filter> cachedFieldValueFilters;
    /**
     * Bits with visible documents bits turned on.
     */
    @Nullable
    FixedBitSet docBits;
    /**
     * Term filter in use.
     */
    @Nullable
    TermFilter termFilter;
    /**
     * Filtered reader context.
     */
    @Nullable
    LeafReaderContext context;
    /**
     * Context of wrapped reader.
     */
    @Nullable
    LeafReaderContext originContext;
    /**
     * Number of visible documents.
     */
    int numDocs;
    /**
     * Highest document number.
     */
    int maxDoc;

    /**
     * Context object constructor.
     */
    FLRContext() {
      this.cachedFieldValueFilters = Collections.synchronizedMap(
          new HashMap<>(15));
    }
  }

  /**
   * Wrapper for a {@link TermsEnum} instance providing filtering.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FilteredTermsEnum
      extends TermsEnum {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(FilteredTermsEnum.class);
    /**
     * Original TermsEnum instance being wrapped.
     */
    private final TermsEnum in;
    /**
     * Context information for FilteredLeafReader.
     */
    private final FLRContext ctx;
    /**
     * Shared {@link DocsEnum} instance.
     */
    @Nullable
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private DocsEnum sharedDocsEnum;
    /**
     * On-time retrieval result of document-frequency and total-term-frequency
     * value.
     */
    @Nullable
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long[] freqs;

    /**
     * Creates a new FilterTermsEnum.
     *
     * @param flr Context information for FilteredLeafReader
     * @param wrap the underlying TermsEnum instance
     */
    FilteredTermsEnum(final FLRContext flr, final TermsEnum wrap) {
      this.in = Objects.requireNonNull(wrap);
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredTermsEnum::new t={}",
            Thread.currentThread().getName());
      }
      this.ctx = flr;
    }

    @Override
    public String toString() {
      return "FilteredTermsEnum [" + this.in + ']';
    }

    @Override
    public SeekStatus seekCeil(final BytesRef term)
        throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredTermsEnum::seekCeil t={}",
            Thread.currentThread().getName());
      }
      // try seek to term
      SeekStatus status = this.in.seekCeil(term);

      // check, if we hit the end of the term list
      // or term is contained in any visible document
      if (status != SeekStatus.END) {
        if (!hasDoc()) {
          // get next term, document visibility checked in next() method
          status = next() == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
        }
      }
      return status;
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
          .docs(this.ctx.docBits,
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
      BytesRef term;

      while ((term = this.in.next()) != null) {
        if (hasDoc() &&
            (this.ctx.termFilter == null ||
                this.ctx.termFilter.isAccepted(this.in, term))) {
          break;
        }
      }
      return term;
    }

    @Override
    public void seekExact(final long ord)
        throws IOException {
      this.in.seekExact(ord);
    }

    @Override
    public BytesRef term()
        throws IOException {
      return this.in.term();
    }

    @Override
    public long ord()
        throws IOException {
      return this.in.ord();
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
     * Get the document-frequency and the total-term-frequency value at the same
     * time.
     *
     * @return Array [docFreq, TTF]
     * @throws IOException Thrown on low-level I/O-errors
     */
    private synchronized long[] freqs()
        throws IOException {
      if (this.freqs == null) {
        this.sharedDocsEnum = this.in.docs(this.ctx.docBits,
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
        return this.in.docs(this.ctx.docBits, reuse, flags);
      } else {
        if (this.ctx.docBits == null) {
          return this.in.docs(liveDocs, reuse, flags);
        }
        final FixedBitSet liveBits = this.ctx.docBits.clone();
        liveBits.and(BitsUtils.bits2FixedBitSet(liveDocs));
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

      final DocsAndPositionsEnum dape;

      if (liveDocs == null) {
        dape = this.in.docsAndPositions(this.ctx.docBits, reuse, flags);
      } else {
        if (this.ctx.docBits == null) {
          dape = this.in.docsAndPositions(liveDocs, reuse, flags);
        } else {
          final FixedBitSet liveBits = this.ctx.docBits.clone();
          liveBits.and(BitsUtils.bits2FixedBitSet(liveDocs));
          dape = this.in.docsAndPositions(liveBits, reuse, flags);
        }
      }
      return dape;
    }
  }

  /**
   * Wrapper for a {@link Terms} instance providing filtering.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FilteredTerms
      extends Terms {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(FilteredTerms.class);
    /**
     * Current field name.
     */
    private final String field;
    /**
     * Cached values of total document/term frequency. Set once it's calculated,
     * otherwise the value is -1.
     */
    private final long[] sumFreqs;
    /**
     * Context information for FilteredLeafReader.
     */
    final FLRContext ctx;
    /**
     * Original Terms instance being wrapped.
     */
    private final Terms in;

    /**
     * Creates a new FilterTerms, passing a FilterFields instance. This gets
     * used, if the global instance has not yet initialized.
     *
     * @param wrap the underlying Terms instance
     * @param fld Current field.
     * @param flr Context information for FilteredLeafReader
     * @param ffInstance FilteredFields instance, if not initialized already
     */
    FilteredTerms(final FLRContext flr, final FilteredFields ffInstance,
        final Terms wrap, final String fld) {
      this.in = Objects.requireNonNull(wrap);
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredTerms::new t={}",
            Thread.currentThread().getName());
      }
      this.ctx = flr;
      this.field = fld;

      // initialized cached frequency values
      long[] sumFreqs = ffInstance.fieldTermsSumCache.get(this.field);
      if (sumFreqs == null) {
        sumFreqs = new long[]{-1L, -1L};
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

      while (termsEnum.next() != null) {
        docsEnum = termsEnum.docs(this.ctx.docBits,
            docsEnum, DocsEnum.FLAG_NONE);
        if (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return "FilteredTerms [" + this.in + ']';
    }

    @Override
    public TermsEnum iterator(@Nullable final TermsEnum reuse)
        throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredTerms::iterator() t={}",
            Thread.currentThread().getName());
      }
      return new FilteredTermsEnum(this.ctx, this.in.iterator(reuse));
    }

    @Override
    public long size() {
      if (LOG.isTraceEnabled()) {
        LOG.trace("@FilteredTerms::size() t={} -- NOT IMPLEMENTED",
            Thread.currentThread().getName());
      }
      return -1L;
    }

    @Override
    public synchronized long getSumTotalTermFreq()
        throws IOException {
      if (LOG.isTraceEnabled()) {
        //noinspection CallToNativeMethodWhileLocked
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
                //noinspection ConstantConditions
                if (FilteredTerms.this.ctx.termFilter == null ||
                    FilteredTerms.this.ctx
                        .termFilter.isAccepted(te, nextTerm)) {
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
        //noinspection CallToNativeMethodWhileLocked
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
                //noinspection ConstantConditions
                if (FilteredTerms.this.ctx.termFilter == null ||
                    FilteredTerms.this.ctx
                        .termFilter.isAccepted(te, nextTerm)) {
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
      Filter f = this.ctx.cachedFieldValueFilters.get(this.field);

      if (f == null) {
        f = new CachingWrapperFilter(new FieldValueFilter(this.field));
        this.ctx.cachedFieldValueFilters.put(this.field, f);
      }

      final DocIdSet docsWithField = f.getDocIdSet(
          this.ctx.originContext, this.ctx.docBits);

      int count = 0;
      if (docsWithField != null) {

        count = DocIdSetUtils.cardinality(docsWithField);
//        final DocIdSetIterator docsWithFieldIt = docsWithField.iterator();
//        if (docsWithFieldIt != null) {
//          while (docsWithFieldIt.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
//            count++;
//          }
//        }
      }
      return count;
    }

    @Override
    public boolean hasFreqs() {
      return this.in.hasFreqs();
    }

    @Override
    public boolean hasOffsets() {
      return this.in.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
      return this.in.hasPositions();
    }

    @Override
    public boolean hasPayloads() {
      return this.in.hasPayloads();
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
}
