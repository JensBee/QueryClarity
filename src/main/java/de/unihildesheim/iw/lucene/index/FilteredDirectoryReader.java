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
import de.unihildesheim.iw.lucene.index.TermFilter.AcceptAll;
import de.unihildesheim.iw.lucene.search.EmptyFieldFilter;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator.OfLong;
import java.util.concurrent.ConcurrentHashMap;
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
   * @throws IOException Thrown on low-level i/o errors
   */
  @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS",
      "LO_APPENDED_STRING_IN_FORMAT_STRING"})
  FilteredDirectoryReader(
      @NotNull final DirectoryReader dirReader,
      @NotNull final SubReaderWrapper wrapper,
      @NotNull final Collection<String> vFields,
      final boolean negate,
      @Nullable final Filter qFilter,
      @NotNull final TermFilter tFilter)
      throws IOException {
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
  protected FilteredDirectoryReader doWrapDirectoryReader(
      final DirectoryReader dirReader)
      throws IOException {
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
  @SuppressWarnings("PublicInnerClass")
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
     * Constant value if no fieldsInfos are available.
     */
    private static final FieldInfos NO_FIELDINFOS =
        new FieldInfos(new FieldInfo[0]);
    /**
     * Set of field names visible. Must be sorted to use {@link
     * Arrays#binarySearch(Object[], Object)}.
     */
    private final String[] fields;
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
     * Fields instance.
     */
    private final FilteredFields fieldsInstance;
    /**
     * The readers ord in the top-level's leaves array
     */
    private final int ord;

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
    FilteredLeafReader(
        @NotNull final LeafReader wrap,
        @NotNull final Collection<String> vFields,
        final boolean negate,
        @Nullable final Filter qFilter,
        @NotNull final TermFilter tFilter)
        throws IOException {
      this.in = wrap;

      if (vFields.isEmpty() && qFilter == null) {
        LOG.warn("No filters specified. " +
            "You should use a plain IndexReader to get better performance.");
      }

      this.ord = wrap.getContext().ord;
      this.flrContext = new FLRContext();

      // all docs are initially live (no deletions allowed)
      this.flrContext.docBits = new FixedBitSet(this.in.maxDoc());
      this.flrContext.docBits.set(0, this.flrContext.docBits.length());

      // reduce the number of visible fields, if desired
      if (vFields.isEmpty()) {
        // all fields are visible
        this.fieldInfos = this.in.getFieldInfos();
        this.fields = StreamSupport.stream(
            this.in.fields().spliterator(), false)
            // array needs to be sorted for binarySearch
            .sorted().toArray(String[]::new);
      } else {
        this.fields = applyFieldFilter(vFields, negate);
        this.fieldInfos = this.fields.length == 0 ? NO_FIELDINFOS :
            new FieldInfos(StreamSupport.stream(
                this.in.getFieldInfos().spliterator(), false)
                .filter(fi -> hasField(fi.name))
                .toArray(FieldInfo[]::new));
      }

      if (qFilter != null) {
        // user document filter gets applied
        applyDocFilter(qFilter);
      }

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

      // number of documents enabled after filtering
      this.flrContext.numDocs = this.flrContext.docBits.cardinality();
      // check, if all bits are turned on
      if (this.flrContext.numDocs == this.flrContext.docBits.length()) {
        this.flrContext.allBitsSet = true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Final state: numDocs={} maxDoc={} fields={}",
            this.flrContext.numDocs, this.flrContext.maxDoc,
            Arrays.toString(this.fields));
      }

      // create context
      this.flrContext.termFilter = tFilter;
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

      String[] fields = StreamSupport.stream(
          this.in.fields().spliterator(), false)
          .filter(f -> negate ^ vFields.contains(f))
          .toArray(String[]::new);

      // Bit-set indicating valid documents (after filtering).
      // A document whose bit is on is valid.
      final FixedBitSet filterBits = new FixedBitSet(this.in.maxDoc());

      if (fields.length == 0) {
        fields = NO_FIELDS;
      } else {
        final List<String> finalFields =
            new ArrayList<>(this.in.fields().size());
        for (final String field : fields) {
          final DocIdSet docsWithField = new EmptyFieldFilter(field)
              .getDocIdSet(this.in.getContext(),
                  null); // isAccepted all docs, no deletions

          // may be null, if no document matches
          finalFields.add(field);
          StreamUtils.stream(docsWithField).forEach(filterBits::set);
        }

        fields = finalFields.isEmpty() ? NO_FIELDS :
            finalFields.toArray(new String[finalFields.size()]);
      }

      // update visible documents bits
      this.flrContext.docBits = filterBits;

      // provide a status message
      LOG.info("Applying field-filter on index-segment ({} -> {})",
          this.flrContext.docBits.cardinality(), filterBits.cardinality());

      if (fields.length > 0) {
        // array needs to be sorted for binarySearch
        Arrays.sort(fields);
      }
      return fields;
    }

    /**
     * Check if a named field is valid (visible or filtered out)
     *
     * @param field Field name to check
     * @return True, if valid (visible), false otherwise
     */
    boolean hasField(final String field) {
      return this.fields.length != 0 &&
          Arrays.binarySearch(this.fields, field) >= 0;
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
          this.in.getContext(), this.flrContext.getDocBitsOrNull()))
          .forEach(filterBits::set);

      // provide a status message
      LOG.info("Applying document-filter on index-segment {} ({} -> {})",
          this.ord,
          this.flrContext.docBits.cardinality(), filterBits.cardinality());
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
    public BitSet getDocsWithField(@NotNull final String field)
        throws IOException {
      if (!hasField(field)) {
        return null;
      }

      // get a bit-set of matching docs from the original reader..
      // AND them with the allowed documents to get only visible matches
      @Nullable final BitSet filteredDocs;
      filteredDocs = BitsUtils.bits2FixedBitSet(
          this.in.getDocsWithField(field));
      if (filteredDocs == null) {
        return null;
      }
      // and only, if there are unset bits
      if (!this.flrContext.allBitsSet) {
        ((FixedBitSet) filteredDocs).and(this.flrContext.docBits);
      }
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
      @Nullable Fields f = this.in.getTermVectors(docID);
      if (f == null) {
        return null;
      }
      // skip doc check in filtered fields, since
      // this fields is a single doc instance
      f = new FilteredFields(this.flrContext, f, true, this.fields);
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
  public static final class Builder
      implements Buildable<FilteredDirectoryReader> {
    /**
     * Original DirectoryReader instance being wrapped.
     */
    final DirectoryReader in;
    /**
     * Query filter.
     */
    @Nullable
    Filter queryFilter;
    /**
     * Term filter.
     */
    @Nullable
    TermFilter termFilter;
    /**
     * Flag indicating, if TermFilter should be negated.
     */
    boolean negateTermFilter;
    /**
     * Fields visible.
     */
    @Nullable
    Collection<String> visibleFields;

    @Override
    public String toString() {
      return "Builder (FilteredDirectoryReader) " + super.toString();
    }

    /**
     * Creates a new builder wrapping a existing DirectoryReader.
     *
     * @param wrap Instance to wrap
     */
    public Builder(@NotNull final DirectoryReader wrap) {
      this.in = wrap;
    }

    /**
     * Sets the query filter to reduce the number of visible documents.
     *
     * @param qFilter Filter instance
     * @return Self reference
     */
    public Builder queryFilter(@Nullable final Filter qFilter) {
      this.queryFilter = qFilter;
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
        @NotNull final Collection<String> vFields) {
      this.visibleFields = new HashSet<>(vFields.size());
      this.visibleFields.addAll(vFields);
      this.negateTermFilter = false;
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
        @NotNull final Collection<String> vFields, final boolean negate) {
      this.visibleFields = new HashSet<>(vFields.size());
      this.visibleFields.addAll(vFields);
      this.negateTermFilter = negate;
      return this;
    }

    /**
     * Sets the term-filter for reducing the visible terms.
     *
     * @param tFilter Filter instance.
     * @return Self reference
     */
    public Builder termFilter(@Nullable final TermFilter tFilter) {
      this.termFilter = tFilter;
      return this;
    }

    /**
     * Creates the FilteredDirectoryReader instance.
     *
     * @return New FilteredDirectoryReader instance
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @NotNull
    @Override
    public FilteredDirectoryReader build()
        throws BuildException {
      if (this.termFilter == null) {
        this.termFilter = new AcceptAll();
      }
      if (this.visibleFields == null) {
        this.visibleFields = Collections.emptySet();
      }

      final SubReaderWrapper srw = new SubReaderWrapper() {
        @Override
        public LeafReader wrap(final LeafReader reader) {
          try {
            assert Builder.this.termFilter != null;
            assert Builder.this.visibleFields != null;
            return new FilteredLeafReader(reader, Builder.this.visibleFields,
                Builder.this.negateTermFilter, Builder.this.queryFilter,
                Builder.this.termFilter);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };

      try {
        return new FilteredDirectoryReader(
            this.in, srw, this.visibleFields,
            this.negateTermFilter, this.queryFilter, this.termFilter);
      } catch (final IOException e) {
        throw new BuildException("Failed to create filtered reader.", e);
      }
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
     * Static value for empty field list.
     */
    private static final String[] NO_FIELDS = {};

    /**
     * Creates a new FilterFields.
     *
     * @param flr Filtered reader context
     * @param wrap Original Fields instance
     * @param skipDocCheck Skips the check, if a doc exists with a given field.
     * Useful in combination with Field instances from TermVectors.
     * @param fld Visible fields
     */
    FilteredFields(
        final FLRContext flr, final Fields wrap, final boolean skipDocCheck,
        final String... fld) {
      this.in = wrap;
      this.ctx = flr;
      if (fld.length == 0) {
        this.fieldTermsSumCache = Collections.emptyMap();
        this.fields = NO_FIELDS;
      } else {
        this.fieldTermsSumCache = new ConcurrentHashMap<>(fld.length << 1);
        // collect visible document fields
        this.fields = getFields(skipDocCheck, fld);
      }
    }

    /**
     * Creates a new FilterFields.
     *
     * @param flr Filtered reader context
     * @param wrap Original Fields instance
     * @param fld Visible fields
     */
    FilteredFields(
        final FLRContext flr, final Fields wrap, final String... fld) {
      this(flr, wrap, false, fld);
    }

    /**
     * Get a list of all visible document fields that have any content.
     *
     * @param skipDocCheck Skip check, if a document exists with a given field.
     * Usable with TermVector Fields instances.
     * @param fld Field initially set visible
     * @return List of available document fields
     */
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private String[] getFields(
        final boolean skipDocCheck, final String... fld) {
      return StreamSupport.stream(this.in.spliterator(), false)
          .filter(f -> {
            try {
              return (Arrays.binarySearch(fld, f) >= 0) &&
                  (skipDocCheck || new FilteredTerms(
                      this.ctx, this, this.in.terms(f), f).hasDoc());
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
      return Arrays.stream(this.fields).iterator();
    }

    @Override
    @Nullable
    public Terms terms(final String field)
        throws IOException {
      return Arrays.binarySearch(this.fields, field) >= 0 ?
          new FilteredTerms(this.ctx, this, this.in.terms(field), field) : null;
    }

    @Override
    public int size() {
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
    public Terms originalTerms(@NotNull final String field)
        throws IOException {
      return this.in.terms(field);
    }
  }

  /**
   * Contextual information for an FilteredLeafReader instance.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FLRContext {
    /**
     * Bits with visible documents bits turned on.
     */
    FixedBitSet docBits;
    /**
     * Term filter in use.
     */
    @Nullable
    TermFilter termFilter;
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
     * True, if all bits are set (all documents are enabled).
     */
    boolean allBitsSet;

    /**
     * Get the bits for all available documents or {@code null} if all documents
     * in index are made available.
     *
     * @return {@code Null}, if all documents in index are available, otherwise
     * a bitset with all enabled documents
     */
    @Nullable
    FixedBitSet getDocBitsOrNull() {
      return this.allBitsSet ? null : this.docBits;
    }
  }

  /**
   * Wrapper for a {@link TermsEnum} instance providing filtering.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FilteredTermsEnum
      extends TermsEnum {
    /**
     * Original TermsEnum instance being wrapped.
     */
    private final TermsEnum in;
    /**
     * Context information for FilteredLeafReader.
     */
    private final FLRContext ctx;
    /**
     * One-time calculated total term-frequency value for the current term.
     * {@code -1}, if not calculated.
     */
    private volatile long ttf;

    /**
     * Creates a new FilterTermsEnum.
     *
     * @param flr Context information for FilteredLeafReader
     * @param wrap the underlying TermsEnum instance
     */
    FilteredTermsEnum(
        @NotNull final FLRContext flr,
        @NotNull final TermsEnum wrap) {
      this.in = wrap;
      this.ctx = flr;
      this.ttf = -1L;
    }

    @Override
    public String toString() {
      return "FilteredTermsEnum [" + this.in + ']';
    }

    @Override
    public boolean seekExact(@NotNull final BytesRef term)
        throws IOException {
      this.ttf = -1L; // reset ttf value, since current term has changed

      return this.in.seekCeil(term) == SeekStatus.FOUND && hasDoc() &&
          (this.ctx.termFilter == null ||
              this.ctx.termFilter.isAccepted(this.in, term()));
    }

    @Override
    public SeekStatus seekCeil(@NotNull final BytesRef term)
        throws IOException {
      this.ttf = -1L; // reset ttf value, since current term has changed
      // try seek to term
      SeekStatus status = this.in.seekCeil(term);

      // check, if we hit the end of the term list
      // or term is contained in any visible document
      while (status != SeekStatus.END) {
        if (hasDoc() && (this.ctx.termFilter == null ||
            this.ctx.termFilter.isAccepted(this.in, term()))) {
          break;
        }
        status = next() == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
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
          .postings(this.ctx.getDocBitsOrNull(), null, (int) PostingsEnum.NONE)
          .nextDoc();
    }

    /**
     * Returns the next term, if any, excluding terms not currently in the
     * visible documents.
     *
     * @return Next term or {@code null}, if there's none left
     * @throws IOException Thrown on low-level I/O-errors
     */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    @Nullable
    public BytesRef next()
        throws IOException {
      this.ttf = -1L; // reset ttf value, since current term has changed
      BytesRef term;

      while ((term = this.in.next()) != null && (!hasDoc() ||
          (this.ctx.termFilter != null &&
              !this.ctx.termFilter.isAccepted(this.in, term)))) {
        // NOP, just skip terms we should ignore
      }
      return term;
    }

    @Override
    public void seekExact(final long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term()
        throws IOException {
      return this.in.term();
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq()
        throws IOException {
      final PostingsEnum pe = this.in.postings(this.ctx.getDocBitsOrNull(),
          null, (int) PostingsEnum.NONE);
      int docFreq = 0;
      while (pe.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        docFreq++;
      }
      return docFreq;
    }

    @Override
    public long totalTermFreq()
        throws IOException {
      if (this.ttf < 0L) {
        long newTTF;
        final PostingsEnum pe;

        if (this.ctx.numDocs > (this.ctx.maxDoc >> 1)) {
          // more than half of the documents are enabled
          // get initial value from all docs
          newTTF = this.in.totalTermFreq();
          // if all bits are set, take the ttf value we've got
          if (!this.ctx.allBitsSet) {
            // more than half of the documents are enabled, so get the
            // total-term-frequency value and subtract disabled document values
            final FixedBitSet nonMatchingDocs = this.ctx.docBits.clone();
            // flip bits
            nonMatchingDocs.flip(0, nonMatchingDocs.length());
            pe = this.in.postings(nonMatchingDocs, null,
                (int) PostingsEnum.FREQS);

            // subtract value for each doc
            while (pe.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              newTTF -= (long) pe.freq();
            }
          }
        } else {
          // less than half of the documents are enabled, so add up all
          // values manually for all enabled documents
//          pe = this.in.postings(this.ctx.docBits, null,
          pe = this.in.postings(this.ctx.getDocBitsOrNull(), null,
              (int) PostingsEnum.FREQS);
          // add up values
          newTTF = 0L;
          while (pe.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            newTTF += (long) pe.freq();
          }
        }
        this.ttf = newTTF;
      }
      return this.ttf;
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public PostingsEnum postings(
        @Nullable final Bits liveDocs,
        @Nullable final PostingsEnum reuse,
        final int flags)
        throws IOException {
      final PostingsEnum postings;
      if (liveDocs == null || liveDocs == this.ctx.getDocBitsOrNull()) {
        postings = this.in.postings(this.ctx.getDocBitsOrNull(), reuse, flags);
      } else {
        final FixedBitSet liveBits = this.ctx.docBits.clone();
        liveBits.and(BitsUtils.bits2FixedBitSet(liveDocs));
        postings = this.in.postings(liveBits, reuse, flags);
      }
      return postings;
    }
  }

  /**
   * Wrapper for a {@link Terms} instance providing filtering.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  static final class FilteredTerms
      extends Terms {
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
    FilteredTerms(
        @NotNull final FLRContext flr,
        @NotNull final FilteredFields ffInstance,
        @NotNull final Terms wrap,
        @NotNull final String fld) {
      this.in = wrap;
      this.ctx = flr;
      this.field = fld;

      // initialized cached frequency values
      @Nullable long[] sumFreqs = ffInstance.fieldTermsSumCache.get(this.field);
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
      final TermsEnum termsEnum = iterator(null);
      PostingsEnum pe = null;

      while (termsEnum.next() != null) {
        pe = termsEnum.postings(this.ctx.getDocBitsOrNull(),
            pe, (int) PostingsEnum.NONE);
        if (pe.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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
      return new FilteredTermsEnum(this.ctx, this.in.iterator(reuse));
    }

    @Override
    public long size() {
      return -1L;
    }

    @Override
    public synchronized long getSumTotalTermFreq()
        throws IOException {
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
      if (this.sumFreqs[0] < 0L) {
        final TermsEnum te = iterator(null);
        this.sumFreqs[0] = StreamSupport.longStream(
            new DocFreqSpliterator(te), false).sum();
      }
      return this.sumFreqs[0];
    }

    @Override
    public int getDocCount()
        throws IOException {
      final DocIdSet docsWithField = new EmptyFieldFilter(this.field)
          .getDocIdSet(this.ctx.originContext, this.ctx.getDocBitsOrNull());
      return DocIdSetUtils.cardinality(docsWithField);
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
      return this.in.iterator(reuse);
    }

    /**
     * Simple spliterator for document-frequency calculation of the current
     * term.
     */
    private static final class DocFreqSpliterator
        implements OfLong {
      /**
       * Wrapped terms enumarator.
       */
      private final TermsEnum te;

      /**
       * Create a new instance using the provided terms iterator.
       *
       * @param te Iterator
       */
      DocFreqSpliterator(@NotNull final TermsEnum te) {
        this.te = te;
      }

      @Override
      public boolean tryAdvance(final LongConsumer action) {
        try {
          final BytesRef nextTerm = this.te.next();
          if (nextTerm == null) {
            return false;
          } else {
            action.accept((long) this.te.docFreq());
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
    }
  }
}
