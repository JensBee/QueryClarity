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
package de.unihildesheim.lucene.index;

import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link IndexDataProvider}. This abstract
 * class creates the basic data structures to handle pre-calculated index data
 * and provides basic accessors functions to those values.
 *
 * The calculation of all term frequency values respect the list of defined
 * document fields. So all values are only calculated for terms found in those
 * fields.
 *
 * The data storage {@link Map} implementations are assumed to be immutable,
 * so stored objects cannot be modified directly and have to be removed and
 * re-added to get modified.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class AbstractIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          AbstractIndexDataProvider.class);

  /**
   * External property prefix.
   */
  protected static final String EXT_PROP_PREFIX = "ext";
  /**
   * Properties timestamp entry name.
   */
  protected static final String PROP_TIMESTAMP = "LM_TIMESTAMP";

  /**
   * Properties persistent stored.
   */
  private Properties storageProp;
  /**
   * Properties file storage path.
   */
  private String propPath;
  /**
   * Properties id prefix.
   */
  private String propId;

  /**
   * Document fields to operate on.
   */
  private String[] fields = new String[0];

  /**
   * Check if all requested fields are available in the current index.
   *
   * @param indexReader Reader to access the index
   */
  protected final void checkFields(final IndexReader indexReader) {
    checkFields(indexReader, this.getFields());
  }

  /**
   * Check if all given fields are available in the current index.
   *
   * @param indexReader Reader to access the index
   * @param fields Fields to check
   */
  protected final void checkFields(final IndexReader indexReader,
          final String[] fields) {
    // get all indexed fields from index - other fields are not of interes here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
            indexReader);

    // check if all requested fields are available
    if (!indexedFields.containsAll(Arrays.asList(fields))) {
      throw new IllegalStateException(MessageFormat.format(
              "Not all requested fields ({0}) "
              + "are available in the current index ({1}) or are not indexed.",
              StringUtils.join(this.getFields(), ","), Arrays.toString(
                      indexedFields.toArray(
                              new String[indexedFields.size()]))));
    }
  }

  @Override
  public final String[] getFields() {
    return this.fields.clone();
  }

  /**
   * Set the document fields this {@link IndexDataProvider} accesses for
   * statics calculation. Note that changing fields while the values are
   * calculated may render the calculation results invalid. You should call
   * {@link AbstractIndexDataProvider#clearData()} to remove any
   * pre-calculated data if fields have changed and recalculate values as
   * needed.
   *
   * @param newFields List of field names
   */
  protected final void setFields(final String[] newFields) {
    if (newFields == null || newFields.length == 0) {
      throw new IllegalArgumentException("Empty fields specified.");
    }
    this.fields = newFields.clone();
  }

  /**
   * Try to read the properties file.
   *
   * @return True, if the file is there, false otherwise
   * @throws IOException Thrown on low-level I/O errors
   */
  protected final boolean loadProperties(final String path, final String id)
          throws
          IOException {
    this.propPath = path;
    this.propId = id;
    this.storageProp = new Properties();
    boolean hasProp;

    final File propFile = new File(this.propPath, this.propId + ".properties");
    try {
      try (FileInputStream propStream = new FileInputStream(propFile)) {
        this.storageProp.load(propStream);
      }

      hasProp = true;
    } catch (FileNotFoundException ex) {
      LOG.trace("Cache meta file " + propFile + " not found.", ex);
      hasProp = false;
    }
    return hasProp;
  }

  /**
   * Save meta information for stored data.
   *
   * @throws IOException If there where any low-level I/O errors
   */
  protected final void saveProperties() throws IOException {
    this.storageProp.
            setProperty(PROP_TIMESTAMP, new SimpleDateFormat(
                            "MM/dd/yyyy h:mm:ss a").format(new Date()));
    final File propFile = new File(this.propPath, this.propId
            + ".properties");
    try (FileOutputStream propFileOut = new FileOutputStream(propFile)) {
      this.storageProp.store(propFileOut, "");
    }
  }

  protected final void setProperty(final String key, final String value) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
    }
    this.storageProp.setProperty(key, value);
  }

  @Override
  public final void setProperty(final String prefix, final String key,
          final String value) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
    }
    this.storageProp.setProperty(EXT_PROP_PREFIX + "_" + prefix + '_' + key,
            value);
  }

  protected final String getProperty(final String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.storageProp.getProperty(key);
  }

  @Override
  public final String getProperty(final String prefix, final String key) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.storageProp.getProperty(EXT_PROP_PREFIX + "_" + prefix + "_"
            + key);
  }

  @Override
  public final void clearProperties() {
    final String extPropPrefix = EXT_PROP_PREFIX
            + "_";
    Iterator<Object> propKeys = this.storageProp.keySet().iterator();
    while (propKeys.hasNext()) {
      if (((String) propKeys.next()).startsWith(extPropPrefix)) {
        propKeys.remove();
      }
    }
  }

  @Override
  public final String getProperty(final String prefix, final String key,
          final String defaultValue) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.storageProp.getProperty(prefix + "_" + key, defaultValue);
  }
}
