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

package de.unihildesheim.iw.lucene.context;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Context providing Lucene index related information.
 * @author Jens Bertram
 */
public final class IndexContext {

  /**
   * Last commit generation id of the Lucene index.
   */
  private long indexLastCommitGeneration;

  /**
   * Index reader for accessing the Lucene index.
   */
  private IndexReader reader;

  /**
   * Private constructor.
   */
  private IndexContext() {
  }

  /**
   * Creates a new instance using a {@link Builder}.
   *
   * @param builder Builder to use
   * @return New instance using {@link Builder} configuration
   */
  private static IndexContext build(final Builder builder)
      throws IOException {
    final IndexContext instance = new IndexContext();
    instance.openReader(builder.getIndexDir());
    return instance;
  }

  /**
   * Tries to open a Lucene IndexReader.
   *
   * @param indexDir Directory of the Lucene index
   * @throws java.io.IOException Thrown on low-level I/O-errors
   */
  private void openReader(final File indexDir)
      throws IOException {
    final Directory directory = FSDirectory.open(indexDir);
    if (!DirectoryReader.indexExists(directory)) {
      throw new IOException("No index found at '" + indexDir.getAbsolutePath()
                            + "'.");
    }
    this.indexLastCommitGeneration = SegmentInfos.getLastCommitGeneration(
        directory);
    this.reader = DirectoryReader.open(directory);
  }

  /**
   * Builder to create a {@link IndexContext}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    /**
     * Index path.
     */
    private String idxPath;

    public void indexPath(final String path) {
      this.idxPath = path;
    }

    /**
     * Get the index directory.
     * @return Index directory
     */
    protected File getIndexDir() {
      return new File(this.idxPath);
    }

    /**
     * Validates the current builder configuration.
     *
     * @throws ContextCreationException Thrown, if not all required parameters
     * are configured or a lower-level exception got caught while initializing
     * the context.
     */
    private void validate()
        throws ContextCreationException {
      if (this.idxPath == null) {
        throw new ContextCreationException("No index path set.");
      } else {
        final File idxPathInstance = new File(this.idxPath);

        if (!idxPathInstance.exists() && !idxPathInstance.isDirectory()) {
          throw new ContextCreationException(
              "Index path '" + this.idxPath + "' does not exist or is not a " +
              "directory."
          );
        }
      }
    }

    /**
     * Build the {@link IndexContext} using the current builder configuration.
     *
     * @return New {@link IndexContext} instance
     * @throws ContextCreationException Thrown, if not all required parameters
     * are configured or a lower-level exception got caught while initializing
     * the context.
     * @throws java.io.IOException Thrown on low-level I/O errors
     */
    public IndexContext build()
        throws ContextCreationException, IOException {
      validate();
      return IndexContext.build(this);
    }
  }
}
