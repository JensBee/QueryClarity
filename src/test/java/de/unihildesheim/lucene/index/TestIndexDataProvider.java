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

import de.unihildesheim.lucene.document.DefaultDocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple {@link IndexDataProvider} implementation for testing purposes.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TestIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Storage meta data.
   */
  private final Properties storageProp = new Properties();

  /**
   * Create a new {@link IndexDataProvider} to access the test
   * {@link MemoryIndex}.
   *
   * @param memoryIdx Memory index instance
   * @throws DocumentModelException Thrown, on errors creating document models
   * @throws IOException Thrown on low-level I/O errors
   */
  public TestIndexDataProvider(final MemoryIndex memoryIdx) throws
          DocumentModelException, IOException {
    this.docModelMap = new ConcurrentHashMap(memoryIdx.getDocumentIds().size());
    this.termFreqMap = new ConcurrentHashMap(memoryIdx.getUniqueTerms().size());
    this.setFields(memoryIdx.getIdxFields());
    this.calculateTermFrequencies(memoryIdx.getReader());
    this.calculateRelativeTermFrequencies();
    this.createDocumentModels(DefaultDocumentModel.class,
            memoryIdx.getReader());
  }

  @Override
  public void dispose() {
    // nothing to do here
  }

  @Override
  public void setProperty(final String prefix, final String key,
          final String value) {
    storageProp.setProperty(prefix + "_" + key, value);
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    return storageProp.getProperty(prefix + "_" + key);
  }

  @Override
  public String getProperty(final String prefix, final String key,
          final String defaultValue) {
    return storageProp.getProperty(prefix + "_" + key, defaultValue);
  }
}
