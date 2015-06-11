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

package de.unihildesheim.iw.cli;

import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.idxStats.IdxStatsDB;
import de.unihildesheim.iw.storage.sql.idxStats.StatsTable;
import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.util.Buildable.BuildException;
import de.unihildesheim.iw.util.Buildable.ConfigurationException;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class DumpIdxStats
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DumpIdxStats.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Lucene document fields to generate statistics for.
   */
  private static final String[] DOC_FIELDS = {"claims", "detd"};

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private DumpIdxStats() {
    super("Dump Lucene index statistics.",
        "Dump detailed Lucene index statistics.");
  }

  public static void main(final String... args)
      throws IOException, SQLException, ClassNotFoundException,
             ConfigurationException, BuildException {
    new DumpIdxStats().runMain(args);
  }

  @SuppressWarnings({"HardcodedLineSeparator",
      "UnnecessarilyQualifiedInnerClassAccess", "ObjectAllocationInLoop"})
  private void runMain(final String... args)
      throws IOException, Buildable.BuildException,
             Buildable.ConfigurationException, SQLException,
             ClassNotFoundException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    LOG.info("Using index at {}", this.cliParams.idxDir.getAbsolutePath());
    if (this.cliParams.targetDb != null) {
      LOG.info("Writing results to {}",
          this.cliParams.targetDb.getAbsoluteFile());
    }

    assert this.cliParams.idxReader != null;
    final int maxDoc = this.cliParams.idxReader.maxDoc();
    if (maxDoc == 0) {
      LOG.error("Empty index.");
      return;
    }

    final DirectoryReader reader = DirectoryReader.open(
        FSDirectory.open(this.cliParams.idxDir.toPath()));

    try (final IdxStatsDB statsDb =
             new IdxStatsDB(this.cliParams.targetDb)) {
      final MetaTable metaTable = new MetaTable();
      final StatsTable statsTable = new StatsTable();
      // write meta-data
      statsDb.createTables(metaTable, statsTable);
      try (final MetaTable.Writer metaWriter =
               metaTable.getWriter(statsDb.getConnection())) {
        metaWriter.addContent(new TableFieldContent(metaTable)
            .setValue(MetaTable.Fields.TABLE_NAME, statsTable.getName())
            .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
      }

      try (final StatsTable.Writer statsWriter =
               statsTable.getWriter(statsDb.getConnection())) {
        // gather stats for all single fields
        for (final String field : DOC_FIELDS) {
          // init filtered reader
          final FilteredDirectoryReader idxReader =
              new FilteredDirectoryReader.Builder(reader)
                  .fields(Collections.singleton(field))
                  .build();

          try (final FDRIndexDataProvider dataProv =
                   new FDRIndexDataProvider.Builder()
                       .indexReader(idxReader)
                       .build()) {

            final TableFieldContent tfc =
                new TableFieldContent(statsTable);

            tfc.setValue(StatsTable.Fields.FIELD, field)
                .setValue(StatsTable.Fields.DOCS, dataProv.getDocumentCount())
                .setValue(StatsTable.Fields.TTF, dataProv.getTermFrequency());

            final Optional<Long> utf = dataProv.getUniqueTermCount();

            if (utf.isPresent()) {
              tfc.setValue(StatsTable.Fields.UTF, utf.get());
            }

            statsWriter.addContent(tfc, false);
          }
        }

        // gather statistics for all significant fields
        final FilteredDirectoryReader idxReader =
            new FilteredDirectoryReader.Builder(reader)
                .fields(Arrays.asList(DOC_FIELDS))
                .build();

        try (final FDRIndexDataProvider dataProv =
                 new FDRIndexDataProvider.Builder()
                     .indexReader(idxReader)
                     .build()) {

          final TableFieldContent tfc =
              new TableFieldContent(statsTable);

          tfc.setValue(StatsTable.Fields.DOCS, dataProv.getDocumentCount())
              .setValue(StatsTable.Fields.TTF, dataProv.getTermFrequency());

          final Optional<Long> utf = dataProv.getUniqueTermCount();

          if (utf.isPresent()) {
            tfc.setValue(StatsTable.Fields.UTF, utf.get());
          }

          statsWriter.addContent(tfc, false);
        }
      }
    }
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Params.class);

    /**
     * Target directory.
     */
    @Option(name = "-out", metaVar = "<file>",
        required = false, usage = "If specified, all data will be written as " +
        "SQLite database to the given file")
    File targetDb;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    @Nullable
    private Directory luceneDir;

    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    @Nullable
    IndexReader idxReader;

    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     *
     * @throws IOException Thrown on low-level i/o-errors
     */
    void check()
        throws IOException {
      assert this.idxDir != null;
      if (this.idxDir.exists()) {
        // check, if path is a directory
        if (!this.idxDir.isDirectory()) {
          throw new IOException("Index path '" + this.idxDir
              + "' exists, but is not a directory.");
        }
        // check, if there's a Lucene index in the path
        this.luceneDir = FSDirectory.open(this.idxDir.toPath());
        if (!DirectoryReader.indexExists(this.luceneDir)) {
          throw new IOException("No index found at index path '" +
              this.idxDir.getCanonicalPath() + "'.");
        }
        this.idxReader = DirectoryReader.open(this.luceneDir);
      } else {
        LOG.error("Index directory '{}' does not exist.", this.idxDir);
        Runtime.getRuntime().exit(-1);
      }

      if (this.targetDb != null) {
        if (this.targetDb.exists()) {
          LOG.error("Database file '{}' exist.", this.targetDb);
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }
}
