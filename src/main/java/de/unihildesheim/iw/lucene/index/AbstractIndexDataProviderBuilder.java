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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Abstract builder to create an {@link IndexDataProvider} instance.
 *
 * @author Jens Bertram
 */
public abstract class AbstractIndexDataProviderBuilder<T extends
    AbstractIndexDataProviderBuilder<T>>
    implements Buildable {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractIndexDataProviderBuilder.class);

  /**
   * Features known.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Feature {
    /**
     * Relative document frequency threshold for classifying common terms.
     * When exceeded for a term it will be skipped.
     */
    COMMON_TERM_THRESHOLD
  }

  /**
   * Values for features supported by the current instance.
   */
  protected Map<Feature, String> supportedFeatures = Collections.EMPTY_MAP;

  /**
   * Set a list of supported features.
   * @param features Features supported by this instance
   */
  protected final void setSupportedFeatures(
      final Feature[] features) {
    this.supportedFeatures = new EnumMap(Feature.class);
    for (final Feature f : features) {
      this.supportedFeatures.put(f, null);
    }
  }

  /**
   * Set a value for a feature.
   * @param f Feature
   * @param value Feature setting value
   * @return Self reference
   */
  public final T setFeature(final Feature f, final String value) {
    if (this.supportedFeatures.containsKey(f)) {
      this.supportedFeatures.put(f, value);
    } else {
      LOG.warn("Feature not supported by current implementation. ({})",
          getThis().getClass().getCanonicalName());
    }
    return getThis();
  }

  /**
   * List of stopwords to use.
   */
  @SuppressWarnings("PackageVisibleField")
  Set<String> stopwords = Collections.emptySet();
  /**
   * List of document fields to use.
   */
  @SuppressWarnings("PackageVisibleField")
  Set<String> documentFields = Collections.emptySet();
  /**
   * {@link IndexReader} to use for accessing the Lucene index.
   */
  @SuppressWarnings("PackageVisibleField")
  IndexReader idxReader;
  /**
   * {@link Directory} instance pointing at the Lucene index.
   */
  private Directory luceneDir;

  /**
   * Get a self-reference of the implementing class.
   *
   * @return Self reference of implementing class instance
   */
  abstract T getThis();

  /**
   * Set a list of stopwords to use by this instance.
   *
   * @param words List of stopwords. May be empty.
   * @return self reference
   */
  public final T stopwords(final Set<String> words) {
    this.stopwords = Objects.requireNonNull(words);
    return getThis();
  }

  /**
   * Set a list of document fields to use by this instance.
   *
   * @param fields List of field names. May be empty.
   * @return self reference
   */
  public final T documentFields(
      final Set<String> fields) {
    Objects.requireNonNull(fields, "Field were null.");
    this.documentFields = new HashSet<>(fields);
    return getThis();
  }

  /**
   * Set and validate the Lucene index directory.
   *
   * @param filePath Path to the Lucene index
   * @return self reference
   * @throws IOException Thrown, if the path is not a directory, no Lucene index
   * was found in the directory or if reading from this directory is not
   * allowed.
   */
  public final T indexPath(final String filePath)
      throws IOException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(filePath, "Index path was null"))) {
      throw new IllegalArgumentException("Index path was empty.");
    }

    final File newIdxDir = new File(filePath);
    if (newIdxDir.exists()) {
      // check, if path is a directory
      if (!newIdxDir.isDirectory()) {
        throw new IOException("Index path '" + newIdxDir.getCanonicalPath()
            + "' exists, but is not a directory.");
      }
      // check, if there's a Lucene index in the path
      this.luceneDir = FSDirectory.open(newIdxDir);
      if (!DirectoryReader.indexExists(this.luceneDir)) {
        throw new IOException("No index found at index path '" + newIdxDir
            .getCanonicalPath() + "'.");
      }
    } else {
      // path does not exist
      throw new IOException(
          "Index path '" + newIdxDir.getCanonicalPath() + "' does " +
              "not exist."
      );
    }
    if (!newIdxDir.canRead()) {
      throw new IOException("Insufficient rights for index directory '"
          + newIdxDir.getCanonicalPath() + "'.");
    }

    return getThis();
  }

  @Override
  public void validate()
      throws ConfigurationException {
    // index reader
    if (this.idxReader == null) {
      if (this.luceneDir == null) {
        throw new IllegalStateException(
            "No IndexReader and no index path was set. Could not open an " +
                "IndexReader."
        );
      }
      try {
        this.idxReader = DirectoryReader.open(this.luceneDir);
      } catch (final IOException e) {
        throw new ConfigurationException("Filed to open Lucene index.", e);
      }
    }

    if (this.idxReader instanceof DirectoryReader) {
      this.luceneDir = ((DirectoryReader) this.idxReader).directory();
    }
  }
}
