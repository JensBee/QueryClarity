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

import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader.Builder;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.lucene.query.IPCClassQuery;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter;
import de.unihildesheim.iw.lucene.search.IPCFieldFilterFunctions.SloppyMatch;
import de.unihildesheim.iw.fiz.storage.sql.MetaTable;
import de.unihildesheim.iw.fiz.storage.sql.Table;
import de.unihildesheim.iw.fiz.storage.sql.TableFieldContent;
import de.unihildesheim.iw.fiz.storage.sql.termData.TermDataDB;
import de.unihildesheim.iw.fiz.storage.sql.termData.TermsTable;
import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.util.Buildable.BuildException;
import de.unihildesheim.iw.util.Buildable.ConfigurationException;
import de.unihildesheim.iw.util.StopwordsFileReader;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TaskObserver;
import de.unihildesheim.iw.util.TaskObserver.TaskObserverMessage;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Commandline utility to dump terms from the Lucene index.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class DumpTermData
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DumpTermData.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private DumpTermData() {
    super("Dump term data.",
        "Dump Document frequency values for every term.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws IOException Thrown on low-level i/o-errors
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown, if connection to the database has failed
   * @throws BuildException Thrown, if building a {@link
   * FilteredDirectoryReader} instance has failed
   */
  public static void main(final String... args)
      throws IOException, SQLException, ClassNotFoundException,
             BuildException, ConfigurationException,
             IPCCode.InvalidIPCCodeException {
    new DumpTermData().runMain(args);
    Runtime.getRuntime().exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws IOException Thrown on low-level i/o-errors
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown, if connection to the database has failed
   * @throws BuildException Thrown, if building a {@link
   * FilteredDirectoryReader} instance has failed
   */
  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  private void runMain(final String... args)
      throws IOException, SQLException, ClassNotFoundException,
             BuildException, Buildable.ConfigurationException,
             IPCCode.InvalidIPCCodeException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    LOG.info("Writing term-data to '{}'.", this.cliParams.dbFile);

    // table manager instance: Target database for term data
    try (final TermDataDB db = new TermDataDB(this.cliParams.dbFile)) {
      // create meta & data table including optional IPC field
      final Table termsTable = new TermsTable(TermsTable.FieldsOptional.IPC);
      final Table metaTable = new MetaTable();
      db.createTables(termsTable, metaTable);

      try (final TermsTable.Writer dataWriter =
               new TermsTable.Writer(db.getConnection())) {

        // write meta-data
        try (final MetaTable.Writer metaWriter =
                 new MetaTable.Writer(db.getConnection())) {
          metaWriter.addContent(new TableFieldContent(metaTable)
              .setValue(MetaTable.Fields.TABLE_NAME, termsTable.getName())
              .setValue(MetaTable.Fields.CMD,
                  StringUtils.join(args, " ")));
        }

        final Set<String> sWords;
        if (this.cliParams.stopFilePattern != null) {
          sWords = CliCommon.getStopwords(this.cliParams.lang,
              this.cliParams.stopFileFormat, this.cliParams.stopFilePattern);
        } else {
          sWords = Collections.emptySet();
        }

        final int numDocs = this.cliParams.idxReader.numDocs();
        if (numDocs == 0) {
          LOG.error("Empty index.");
          return;
        }

        final AtomicLong count = new AtomicLong(0L);

        try (TaskObserver obs = new TaskObserver(
            new TaskObserverMessage() {
              @Override
              public void call(@NotNull final TimeMeasure tm) {
                LOG.info("Collected {} terms after {}.",
                    NumberFormat.getIntegerInstance().format(count.get()),
                    tm.getTimeString());
              }
            }).start()) {
          // normalize some parameters
          final String langName =
              StringUtils.lowerCase(this.cliParams.lang);
          final String fieldName = this.cliParams.field;
          final Terms terms = MultiFields.getTerms(
              this.cliParams.idxReader, this.cliParams.field);

          TermsEnum termsEnum = TermsEnum.EMPTY;
          BytesRef term;
          if (terms != null) {
            termsEnum = terms.iterator(termsEnum);
            term = termsEnum.next();

            while (term != null) {
              final String termStr = term.utf8ToString();
              if (!sWords.contains(termStr.toLowerCase())) {
                final double docFreq = (double) termsEnum.docFreq();
                if (docFreq > 0d) {
                  final double relDocFreq = docFreq / (double) numDocs;

                  if (relDocFreq > this.cliParams.threshold) {
                    @SuppressWarnings("ObjectAllocationInLoop")
                    final TableFieldContent tfc =
                        new TableFieldContent(termsTable);
                    tfc.setValue(TermsTable.Fields.TERM, termStr);
                    tfc.setValue(TermsTable.Fields.DOCFREQ_REL, relDocFreq);
                    tfc.setValue(TermsTable.Fields.DOCFREQ_ABS, docFreq);
                    tfc.setValue(TermsTable.Fields.LANG, langName);
                    tfc.setValue(TermsTable.Fields.FIELD, fieldName);
                    if (this.cliParams.ipcRec != null) {
                      tfc.setValue(
                          TermsTable.FieldsOptional.IPC,
                          this.cliParams.ipcRec.toFormattedString());
                    }
                    dataWriter.addContent(tfc, false);
                    count.incrementAndGet();
                  }
                }
              }
              term = termsEnum.next();
            }
          }
          obs.stop();
        }
        LOG.info("Total of {} terms collected.",
            NumberFormat.getIntegerInstance().format(count));
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
     * Target file file for writing term data.
     */
    @Option(name = "-dbfile", metaVar = "FILE", required = true,
        usage = "SQLite database file. Will be created, if not found.")
    File dbFile;

    /**
     * Stopwords file format.
     */
    @Option(name = "-stop-format", metaVar = "(plain|snowball)",
        required = false, usage =
        "Format of the stopwords file. 'plain' for a simple list of " +
            "each stopword per line. 'snowball' for a list of words and " +
            "comments starting with '|'. Defaults to 'plain'.")
    String stopFileFormat = "plain";

    /**
     * IPC code.
     */
    @Nullable
    @Option(name = "-ipc", metaVar = "IPC", required = false,
        usage = "IPC-code (fragment) to filter returned codes.")
    String ipc = null;
    @Nullable
    IPCCode.IPCRecord ipcRec = null;

    /**
     * Default separator char.
     */
    @Option(name = "-grpsep", metaVar = "[separator char]",
        required = false,
        usage = "Char to use for separating main- and sub-group.")
    char sep = Parser.DEFAULT_SEPARATOR;

    /**
     * Allow zero padding.
     */
    @Option(name = "-zeropad", required = false,
        usage = "Allows padding of missing information with zeros.")
    boolean zeroPad = false;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    private Directory luceneDir;

    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    FilteredDirectoryReader idxReader;

    /**
     * Document-field to query.
     */
    @Option(name = "-field", metaVar = "field name", required = false,
        handler = StringArrayOptionHandler.class,
        usage = "Document field to query. If not specified both '" +
            LUCENE_CONF.FLD_CLAIMS + "' and '" +
            LUCENE_CONF.FLD_DETD + "' will be queried.")
    String field;

    /**
     * Pattern for stopwords files.
     */
    @Nullable
    @Option(name = "-stop", metaVar = "pattern", required = false,
        usage = "File naming pattern for stopword lists. " +
            "The pattern will be suffixed by '_<lang>.txt'. Stopword files " +
            "are expected to be UTF-8 encoded.")
    String stopFilePattern;

    /**
     * Document frequency threshold.
     */
    @Option(name = "-threshold", metaVar = "float", required = false,
        usage = "Document frequency threshold. If this is exceeded a term " +
            "will be treated as being too common (means gets skipped). " +
            "Default: 0")
    double threshold = 0d;

    /**
     * Single language.
     */
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process for the defined language.")
    String lang;

    /**
     * Empty constructor to allow access from parent class.
     */
    Params() {
      // empty
    }

    /**
     * Check, if the defined files and directories are available.
     *
     * @throws IOException Thrown on low-level i/o-errors
     * @throws BuildException Thrown, if a {@link FilteredDirectoryReader}
     * failed to be build
     */
    void check()
        throws IOException, BuildException, IPCCode.InvalidIPCCodeException {
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

        final Builder idxReaderBuilder = new Builder(DirectoryReader.open(
            this.luceneDir));

        // create field filter
        if (this.field == null) {
          this.field = LUCENE_CONF.FLD_CLAIMS + ", " + LUCENE_CONF.FLD_DETD;
          LOG.debug("No fields specified. Using defaults: {}.", this.field);
          idxReaderBuilder.fields(Arrays.asList(
              LUCENE_CONF.FLD_CLAIMS,
              LUCENE_CONF.FLD_DETD));
        } else {
          this.field = StringUtils.lowerCase(this.field);
          idxReaderBuilder.fields(Collections.singleton(this.field));
        }

        if (this.ipc != null) {
          // create ipc query filter
          final Parser ipcParser = new Parser();
          ipcParser.separatorChar(this.sep);
          ipcParser.allowZeroPad(this.zeroPad);

          this.ipcRec = ipcParser.parse(this.ipc);
          final BooleanQuery bq = new BooleanQuery();
          final Pattern rx_ipc = Pattern.compile(
              this.ipcRec.toRegExpString(this.sep));
          if (LOG.isDebugEnabled()) {
            LOG.debug("IPC regExp: rx={} pat={}",
                this.ipcRec.toRegExpString(this.sep),
                rx_ipc);
          }

          bq.add(new QueryWrapperFilter(
              IPCClassQuery.get(this.ipcRec, this.sep)), Occur.MUST);
          bq.add(new QueryWrapperFilter(
              new IPCFieldFilter(
                  new SloppyMatch(this.ipcRec), ipcParser
              )), Occur.MUST);
          idxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
        }

        this.idxReader = idxReaderBuilder.build();

//        if (this.ipc == null) {
//          if (this.field == null) {
//            this.idxReader = reader;
//          }
//        } else {
//          final Parser ipcParser = new Parser();
//          ipcParser.separatorChar(this.sep);
//          ipcParser.allowZeroPad(this.zeroPad);
//
//          final Builder
//              idxReaderBuilder = new Builder(reader);
//          this.ipcRec = ipcParser.parse(this.ipc);
//          final BooleanQuery bq = new BooleanQuery();
//          final Pattern rx_ipc = Pattern.compile(
//              this.ipcRec.toRegExpString(this.sep));
//          if (LOG.isDebugEnabled()) {
//            LOG.debug("IPC regExp: rx={} pat={}",
//                this.ipcRec.toRegExpString(this.sep),
//                rx_ipc);
//          }
//
//          bq.add(new QueryWrapperFilter(
//              IPCClassQuery.get(this.ipcRec, this.sep)), Occur.MUST);
//          bq.add(new QueryWrapperFilter(
//              new IPCFieldFilter(
//                  new SloppyMatch(this.ipcRec), ipcParser
//              )), Occur.MUST);
//          idxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
//          this.idxReader = idxReaderBuilder.build();
//        }
      } else {
        LOG.error("Index directory '{}' does not exist.", this.idxDir);
        Runtime.getRuntime().exit(-1);
      }
      if (StopwordsFileReader.getFormatFromString(
          this.stopFileFormat) == null) {
        LOG.error(
            "Unknown stopwords file format '{}'.", this.stopFileFormat);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
