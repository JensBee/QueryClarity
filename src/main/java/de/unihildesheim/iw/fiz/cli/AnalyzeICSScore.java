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

package de.unihildesheim.iw.fiz.cli;

import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.fiz.SimpleTermFilter;
import de.unihildesheim.iw.fiz.storage.sql.ICSAnalyze.FeedbackDocsTable;
import de.unihildesheim.iw.fiz.storage.sql.ICSAnalyze.FeedbackTermsTable;
import de.unihildesheim.iw.fiz.storage.sql.ICSAnalyze.ICSAnalyzeDB;
import de.unihildesheim.iw.fiz.storage.sql.ICSAnalyze.TermStatsTable;
import de.unihildesheim.iw.fiz.storage.sql.KeyValueTable;
import de.unihildesheim.iw.fiz.storage.sql.MetaTable;
import de.unihildesheim.iw.fiz.storage.sql.TableFieldContent;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.query.IPCClassQuery;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ClarityScoreCalculationException;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTuple;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScore;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScore.Result;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScoreConfiguration;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter;
import de.unihildesheim.iw.lucene.search.IPCFieldFilterFunctions.SloppyMatch;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.Buildable.BuildableException;
import de.unihildesheim.iw.util.GlobalConfiguration;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class AnalyzeICSScore
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(ScoreICS.class);
  /**
   * Object wrapping commandline options.
   */
  final Params cliParams = new Params();
  /**
   * Analyzer for query tokenizing.
   */
  private Analyzer analyzer;
  /**
   * Database connection.
   */
  private Connection con;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private AnalyzeICSScore() {
    super("Score terms & sentences for ICS scorer.",
        "Score terms & sentences for ICS scorer.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String... args)
      throws Exception {
    new AnalyzeICSScore().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   */
  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  private void runMain(@NotNull final String... args)
      throws Exception {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    final int maxBoolClauses = GlobalConfiguration.conf().getAndAddInteger(
        GlobalConfiguration.DefaultKeys.MAX_BOOL_CLAUSES.toString(), 2048);
    LOG.info("Raising boolean max clause count to {}.", maxBoolClauses);
    BooleanQuery.setMaxClauseCount(maxBoolClauses);

    if (this.cliParams.ipc != null) {
      final IPCCode.Parser ipcParser = new IPCCode.Parser();
      ipcParser.separatorChar(this.cliParams.sep);
      ipcParser.allowZeroPad(this.cliParams.zeroPad);

      this.cliParams.ipcRec = ipcParser.parse(this.cliParams.ipc);
    } else {
      this.cliParams.ipcRec = null;
    }

    try (final ICSAnalyzeDB analyzeDb =
             new ICSAnalyzeDB(this.cliParams.dbTarget)) {
      final MetaTable metaTable = new MetaTable();
      final FeedbackDocsTable docsTable = new FeedbackDocsTable();
      final FeedbackTermsTable termsTable = new FeedbackTermsTable();
      final TermStatsTable termStatsTable = new TermStatsTable();
      final KeyValueTable kvTable = new KeyValueTable();

      analyzeDb.createTables(
          metaTable, kvTable, docsTable, termsTable, termStatsTable);
      this.con = analyzeDb.getConnection();

      // write meta-data
      try (final MetaTable.Writer metaWriter =
               metaTable.getWriter(this.con)) {
        metaWriter.addContent(new TableFieldContent(metaTable)
            .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
      }

      try (final KeyValueTable.Writer kvWriter =
               kvTable.getWriter(this.con)) {
        kvWriter.addContent(new TableFieldContent(kvTable)
            .setValue(KeyValueTable.Fields.KEY, "query")
            .setValue(KeyValueTable.Fields.VALUE, this.cliParams.query));
      }

      // lucene analyzer
      this.analyzer = LanguageBasedAnalyzers.createInstance(
          this.cliParams.language, CharArraySet.EMPTY_SET);

      runScore(analyzeDb, this.cliParams.query,
          termStatsTable, docsTable, termsTable, kvTable);
    }
  }

  private void runScore(
      @NotNull final ICSAnalyzeDB targetDb,
      @NotNull final String query,
      @NotNull final TermStatsTable termStatsTable,
      @NotNull final FeedbackDocsTable docsTable,
      @NotNull final FeedbackTermsTable termsTable,
      @NotNull final KeyValueTable kvTable)
      throws BuildableException,
             SQLException,
             ClarityScoreCalculationException {

    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}",
        this.cliParams.language, this.cliParams.docFields);

    // init filtered reader
    final FilteredDirectoryReader.Builder
        idxReaderBuilder = new FilteredDirectoryReader
        .Builder(this.cliParams.idxReader)
        .fields(this.cliParams.docFields);

    // Should we include IPC-data?
    final boolean includeIPC = this.cliParams.ipcRec != null;

    // check, if we should filter by ipc
    final String ipcName;
    if (includeIPC) {
      ipcName = this.cliParams.ipcRec.toFormattedString(this.cliParams.sep);
      final IPCCode.Parser ipcParser = new IPCCode.Parser();
      final BooleanQuery bq = new BooleanQuery();
      final Pattern rx_ipc = Pattern.compile(
          this.cliParams.ipcRec.toRegExpString(this.cliParams.sep));
      if (LOG.isDebugEnabled()) {
        LOG.debug("IPC regExp: rx={} pat={}",
            this.cliParams.ipcRec.toRegExpString(this.cliParams.sep),
            rx_ipc);
      }

      bq.add(new QueryWrapperFilter(
              IPCClassQuery.get(this.cliParams.ipcRec, this.cliParams.sep)),
          Occur.MUST);
      bq.add(new QueryWrapperFilter(
          new IPCFieldFilter(
              new SloppyMatch(this.cliParams.ipcRec),
              ipcParser)), Occur.MUST);
      idxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
    } else {
      ipcName = "";
    }

    if (this.cliParams.termFilter) {
      LOG.info("Using SimpleTermFilter instance!");
      idxReaderBuilder.termFilter(new SimpleTermFilter());
    }

    // finally build the reader
    final FilteredDirectoryReader idxReader = idxReaderBuilder.build();

    // finally build the data provider
    try (final FDRIndexDataProvider dataProv = new FDRIndexDataProvider
        .Builder()
        .indexReader(idxReader)
        .build()) {

      // initialize scorer configuration
      final ImprovedClarityScoreConfiguration icsc = new
          ImprovedClarityScoreConfiguration();
      icsc.setDocumentModelSmoothingParameter(
          this.cliParams.p_docModSmoothing);
      icsc.setFeedbackTermSelectionThreshold(
          this.cliParams.p_fbTsMin, this.cliParams.p_fbTsMax);
      icsc.setMaxFeedbackDocumentsCount(this.cliParams.p_fbMax);
      // create scorer
      final ImprovedClarityScore ics = new ImprovedClarityScore.Builder()
          .indexDataProvider(dataProv)
          .indexReader(idxReader)
          //.feedbackProvider(new CommonTermsFeedbackProvider())
          .configuration(icsc)
          .analyzer(this.analyzer)
          .build();

      final Result result = ics.calculateClarity(query);

      if (result.isEmpty()) {
        if (result.getEmptyReason().isPresent()) {
          LOG.info("Result is empty. Reason: {}",
              result.getEmptyReason().get());
        } else {
          LOG.info("Result is empty. (No reason given)");
        }
      } else {
        int skippedTerms = 0;

        try (final KeyValueTable.Writer kvWriter =
                 kvTable.getWriter(this.con)) {
          kvWriter.addContent(new TableFieldContent(kvTable)
              .setValue(KeyValueTable.Fields.KEY, "score")
              .setValue(KeyValueTable.Fields.VALUE, result.getScore()));
        }

        // write feedback document ids
        final Optional<BitSet> fbDocs = result.getFeedbackDocIds();
        if (fbDocs.isPresent()) {
          try (final FeedbackDocsTable.Writer writer =
                   docsTable.getWriter(this.con)) {
            StreamUtils.stream(fbDocs.get()).forEach(bit -> {
              try {
                writer.addContent(new TableFieldContent(docsTable)
                    .setValue(FeedbackDocsTable.Fields.DOC_ID, bit));
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
          }
        } else {
          LOG.warn("No feedback documents provided by scorer.");
        }

        // term related data
        final Optional<ScoreTuple.TupleType> tupleType =
            result.getScoreTuple();
        if (tupleType.isPresent()) {
          final Object[] tuples;
          if (tupleType.get() == ScoreTuple.TupleType.HIGH_PRECISION) {
            tuples = result.getScoreTupleHighPrecision().get();
          } else if (tupleType.get() == ScoreTuple.TupleType.LOW_PRECISION) {
            tuples = result.getScoreTupleLowPrecision().get();
          } else {
            LOG.error("Unknown score tuple format.");
            throw new IllegalStateException();
          }

          // write feedback terms and term score tuples
          final int tupleCount = tuples.length;
          for (Object tuple1 : tuples) {
            final ScoreTuple tuple = (ScoreTuple) tuple1;

            if (tuple.term.isPresent()) {
              final int termRef = getTermRef(
                  dataProv, targetDb, termStatsTable, tuple.term.get());

              try (final FeedbackTermsTable.Writer writer =
                       termsTable.getWriter(this.con)) {
                writer.addContent(new TableFieldContent(termsTable)
                    .setValue(FeedbackTermsTable.Fields.TERM_REF, termRef)
                    .setValue(FeedbackTermsTable.Fields.Q_MODEL,
                        tuple.getQueryModel().doubleValue())
                    .setValue(FeedbackTermsTable.Fields.C_MODEL,
                        tuple.getCollectionModel().doubleValue()));
              }
            } else {
              LOG.warn("Term missing from score tuple. Skipping term.");
              skippedTerms++;
            }
          }
        } else {
          LOG.error("No score tuples returned from scorer.");
          throw new IllegalStateException();
        }

        LOG.info("Result: skipped terms={}", skippedTerms);
      }
    }
  }

  Optional<Integer> retrieveTermRef(
      @NotNull final Statement stmt,
      @NotNull final String term)
      throws SQLException {
    stmt.execute("select id from " + TermStatsTable.TABLE_NAME +
        " where term='" + term + "';");

    final ResultSet rs = stmt.getResultSet();
    if (rs.next()) { // there should be only one entry - first wins
      return Optional.of(rs.getInt(1));
    }
    return Optional.empty();
  }

  int getTermRef(
      @NotNull final FDRIndexDataProvider dataProv,
      @NotNull final ICSAnalyzeDB targetDb,
      @NotNull final TermStatsTable termStatsTable,
      @NotNull final BytesRef term)
      throws SQLException {
    final Statement stmt = this.con.createStatement();
    final String termStr = term.utf8ToString();

    Optional<Integer> id = retrieveTermRef(stmt, termStr);
    if (!id.isPresent()) {
      try (final TermStatsTable.Writer writer =
               termStatsTable.getWriter(this.con)) {
        writer.addContent(new TableFieldContent(termStatsTable)
            .setValue(TermStatsTable.Fields.TERM, termStr)
            .setValue(TermStatsTable.Fields.DF_REL,
                dataProv.getRelativeDocumentFrequency(term))
            .setValue(TermStatsTable.Fields.DF,
                dataProv.getDocumentFrequency(term))
            .setValue(TermStatsTable.Fields.TF_REL,
                dataProv.getRelativeTermFrequency(term))
            .setValue(TermStatsTable.Fields.TF,
                dataProv.getTermFrequency(term)));
      }
      // retrieve new entry
      id = retrieveTermRef(stmt, termStr);
    }

    if (!id.isPresent()) {
      throw new IllegalStateException("Failed to create term entry in " +
          "database.");
    }

    return id.get();
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Params.class);

    /**
     * Language to process.
     */
    @Option(name = "-lang", metaVar = "<language>", required = true,
        usage = "Process for the defined language.")
    String lang;
    /**
     * Language instance created from parameter.
     */
    LanguageBasedAnalyzers.Language language;

    /**
     * Language to process.
     */
    @Option(name = "-query", metaVar = "<query>", required = true,
        usage = "Query to run.")
    String query;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * {@link DirectoryReader} to use for accessing the Lucene index.
     */
    DirectoryReader idxReader;
    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    private Directory luceneDir;

    /**
     * Target database file file.
     */
    @Option(name = "-targetdb", metaVar = "<database file>", required = true,
        usage = "Target SQLite database.")
    File dbTarget;

    /**
     * Use term-filter?
     */
    @Option(name = "-termfilter", required = false,
        usage = "Use the SimpleTermFilter.")
    boolean termFilter = false;

    /**
     * Document-fields to query.
     */
    @Option(name = "-fields", metaVar = "list", required = true,
        handler = StringArrayOptionHandler.class,
        usage = "List of document fields separated by spaces to query. ")
    String[] fields;
    /**
     * unique list of fields specified.
     */
    Collection<String> docFields;

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
    char sep = IPCCode.Parser.DEFAULT_SEPARATOR;

    /**
     * Allow zero padding.
     */
    @Option(name = "-zeropad", required = false,
        usage = "Allows padding of missing information with zeros.")
    boolean zeroPad = false;


    @Option(name = "-dms", required = true,
        usage = "Document model smoothing parameter (int).")
    Integer p_docModSmoothing;

    @Option(name = "-fbmax", required = true,
        usage = "Maximum number of feedback documents (int).")
    Integer p_fbMax;

    @Option(name = "-fbtsmin", required = true,
        usage = "Feedback documents: minimum docFreq threshold (double).")
    Double p_fbTsMin;

    @Option(name = "-fbtsmax", required = true,
        usage = "Feedback documents: maximum docFreq threshold (double).")
    Double p_fbTsMax;

    /**
     * Empty constructor to allow access from parent class.
     */
    Params() {
      // empty
    }

    /**
     * Check commandline parameters.
     *
     * @throws IOException Thrown on low-level i/o-errors or a Lucene index was
     * not found at the specified location
     */
    void check()
        throws IOException {
      if (this.query.trim().isEmpty()) {
        throw new IllegalArgumentException("Empty query");
      }

      // check for database
      if (this.dbTarget.exists()) {
        throw new IllegalStateException(
            "Target database " + this.dbTarget + " exists or is a directory.");
      }


      this.docFields = new HashSet<>(this.fields.length);
      Collections.addAll(this.docFields, this.fields);

      // check lucene index
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
        LOG.error("Index directory'" + this.idxDir + "' does not exist.");
        System.exit(-1);
      }

      // check language parameter
      // check, if we have an analyzer
      if (!LanguageBasedAnalyzers.hasAnalyzer(this.lang)) {
        throw new IllegalArgumentException(
            "No analyzer for language '" + this.lang + "'.");
      }
      this.language = LanguageBasedAnalyzers.getLanguage(this.lang);
      if (this.language == null) {
        throw new IllegalStateException("Unknown or unsupported language " +
            '(' + this.lang + ").");
      }
    }
  }
}

