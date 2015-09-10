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
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.query.IPCClassQuery;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.BM25Plus;
import de.unihildesheim.iw.lucene.scoring.data.CommonTermsFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter;
import de.unihildesheim.iw.lucene.search.IPCFieldFilterFunctions;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.fiz.storage.sql.MetaTable;
import de.unihildesheim.iw.fiz.storage.sql.TableFieldContent;
import de.unihildesheim.iw.fiz.storage.sql.scoringData
    .BM25SentenceScoringResultTable;
import de.unihildesheim.iw.fiz.storage.sql.scoringData.BM25TermScoringResultTable;
import de.unihildesheim.iw.fiz.storage.sql.scoringData.ScoringDataDB;
import de.unihildesheim.iw.fiz.storage.sql.scoringData.SentenceScoringTable;
import de.unihildesheim.iw.fiz.storage.sql.scoringData.TermScoringTable;
import de.unihildesheim.iw.fiz.storage.sql.termData.TermsTable;
import de.unihildesheim.iw.util.GlobalConfiguration;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TaskObserver;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRefArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class ScoreBM25
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(ScoreBM25.class);
  /**
   * Object wrapping commandline options.
   */
  final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ScoreBM25() {
    super("Score terms & sentences using BM25 scorer.",
        "Score terms & sentences using BM25 scorer.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String... args)
      throws Exception {
    new ScoreBM25().runMain(args);
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

    try (final ScoringDataDB scoringDb =
             new ScoringDataDB(this.cliParams.dbScoring)) {
      // pre-check for scoring-data
      final boolean scoreTerms = scoringDb.hasTable(
          TermScoringTable.TABLE_NAME);
      final boolean scoreSentences = scoringDb.hasTable(
          SentenceScoringTable.TABLE_NAME);

      if (!scoreTerms && !scoreSentences) {
        throw new IllegalStateException(
            "Nothing to score. No terms & sentences found in database.");
      }

      // create result tables
      BM25TermScoringResultTable termTable = null;
      BM25SentenceScoringResultTable sentenceTable = null;
      if (scoreTerms) {
        termTable = new BM25TermScoringResultTable();
        scoringDb.createTables(termTable);
      }
      if (scoreSentences) {
        sentenceTable = new BM25SentenceScoringResultTable();
        scoringDb.createTables(sentenceTable);
      }

      final MetaTable metaTable = new MetaTable();
      // write meta-data
      scoringDb.createTables(metaTable);
      try (final MetaTable.Writer metaWriter =
               metaTable.getWriter(scoringDb.getConnection())) {
        if (scoreTerms) {
          metaWriter.addContent(new TableFieldContent(metaTable)
              .setValue(MetaTable.Fields.TABLE_NAME, termTable.getName())
              .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
        }
        if (scoreSentences) {
          metaWriter.addContent(new TableFieldContent(metaTable)
              .setValue(MetaTable.Fields.TABLE_NAME, sentenceTable.getName())
              .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
        }
        metaWriter.commit();
      }

      runScoring(scoringDb, termTable, sentenceTable);
    }
  }

  private void runScoring(
      @NotNull final ScoringDataDB scoringDb,
      @Nullable final BM25TermScoringResultTable termTable,
      @Nullable final BM25SentenceScoringResultTable sentenceTable)
      throws Exception {
    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}",
        this.cliParams.language, this.cliParams.docFields);

    // init filtered reader
    final FilteredDirectoryReader.Builder
        idxReaderBuilder = new FilteredDirectoryReader
        .Builder(this.cliParams.idxReader)
        .fields(this.cliParams.docFields);

    // check, if we should filter by ipc
    if (this.cliParams.ipcRec != null) {
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
          BooleanClause.Occur.MUST);
      bq.add(new QueryWrapperFilter(
          new IPCFieldFilter(
              new IPCFieldFilterFunctions.SloppyMatch(this.cliParams.ipcRec),
              ipcParser)), BooleanClause.Occur.MUST);
      idxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
    }

    // finally build the reader
    final FilteredDirectoryReader idxReader = idxReaderBuilder.build();

    // Should we include IPC-data?
    final boolean includeIPC = this.cliParams.ipcRec != null;

    final String ipcName;
    if (includeIPC) {
      ipcName = this.cliParams.ipcRec.toFormattedString(this.cliParams.sep);
    } else {
      ipcName = "";
    }

    try (final FDRIndexDataProvider dataProv = new FDRIndexDataProvider
        .Builder()
        .indexReader(idxReader)
        .build()) {
      // normalize some parameters
      final String langName = this.cliParams.language.toString();
      // fields being queried
      final String qFields = StringUtils.join(this.cliParams.docFields, ",");

      final Statement stmt = scoringDb.getConnection().createStatement();
      final String[] scoringType = {"none"};
      final AtomicInteger currentItemCount = new AtomicInteger(0);
      final Analyzer analyzer = LanguageBasedAnalyzers.createInstance(
          this.cliParams.language, CharArraySet.EMPTY_SET);

      // initialize feedback provider
      final FeedbackProvider fbProv = new CommonTermsFeedbackProvider();
      fbProv.dataProvider(dataProv)
          .indexReader(idxReader)
          .amount(this.cliParams.amount)
          .analyzer(analyzer)
          .fields(dataProv.getDocumentFields());

      final BM25Plus bm25Calc = new BM25Plus(dataProv);

      try (TaskObserver obs = new TaskObserver(
          new TaskObserver.TaskObserverMessage() {
            @Override
            public void call(@NotNull final TimeMeasure tm) {
              LOG.info("BM25-Scorer is scoring {} ({} scored) (runtime: {}). " +
                      "lang={} ipc={} field={}",
                  scoringType, currentItemCount.get(),
                  tm.getTimeString(),
                  langName, ipcName, ScoreBM25.this.cliParams.docFields);
            }
          })) {
        obs.start();

        if (sentenceTable != null) {
          scoringType[0] = "sentences";
          // query for data
          final String querySQL = "select " +
              "s." + SentenceScoringTable.Fields.ID + ", " +
              "s." + SentenceScoringTable.Fields.SENTENCE + ", " +
              "t." + TermScoringTable.Fields.LANG + ' ' +
              "from " + SentenceScoringTable.TABLE_NAME + " s " +
              "inner join " +
              TermScoringTable.TABLE_NAME + " t " +
              "on (s." + SentenceScoringTable.Fields.TERM_REF +
              "= t." + TermScoringTable.Fields.ID + ") " +
              "where t." + TermScoringTable.Fields.LANG + "='" +
              langName + "' and t." + TermsTable.FieldsOptional.IPC +
              (includeIPC ?
                  " like '" + ipcName + "%'" : // filter by ipc
                  " is null;"); // ipc must not be present
          if (LOG.isDebugEnabled()) {
            LOG.debug("querySQL: {}", querySQL);
          }
          stmt.execute(querySQL);
          final ResultSet rs = stmt.getResultSet();

          try (final BM25SentenceScoringResultTable.Writer sentenceWriter =
                   sentenceTable.getWriter(scoringDb.getConnection())) {
            currentItemCount.set(0);
            while (rs.next()) {
              final Integer sentId = rs.getInt(1);
              final String sent = rs.getString(2);

              if (sent == null) {
                throw new IllegalStateException("Sentence was null.");
              }

              @SuppressWarnings("ObjectAllocationInLoop")
              final TableFieldContent tfc =
                  new TableFieldContent(sentenceTable);
              tfc.setValue(
                  BM25SentenceScoringResultTable.Fields.SENT_REF, sentId);
              tfc.setValue(
                  BM25SentenceScoringResultTable.Fields.Q_FIELDS, qFields);

              if (includeIPC) {
                tfc.setValue(
                    BM25SentenceScoringResultTable.Fields.Q_IPC, ipcName);
              }

              currentItemCount.incrementAndGet();

              // ---
              final BytesRefArray queryTerms =
                  QueryUtils.tokenizeQuery(sent, analyzer, dataProv);
              // check query term extraction result
              if (queryTerms.size() <= 0) {
                LOG.warn(
                    "Tokenized query ist empty! Skipping B25+ score. q=[{}]",
                    sent);
                tfc.setValue(
                    BM25SentenceScoringResultTable.Fields.DOC_ID, "-1");
                tfc.setValue(
                    BM25SentenceScoringResultTable.Fields.SCORE, "-1");
                // write result
                sentenceWriter.addContent(tfc, false);
              } else {
                final DocIdSet feedbackDocIds = fbProv.query(queryTerms).get();
                StreamUtils.stream(feedbackDocIds).forEach(docId -> {
                  final double score =
                      bm25Calc.score(docId, queryTerms).doubleValue();
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("BM25+ q=[{}] docid={} score={}",
                        sent, docId, score);
                  }
                  tfc.setValue(
                      BM25SentenceScoringResultTable.Fields.DOC_ID, docId);
                  tfc.setValue(
                      BM25SentenceScoringResultTable.Fields.SCORE,
                      score);
                  // write result
                  try {
                    sentenceWriter.addContent(tfc, false);
                  } catch (final SQLException e) {
                    LOG.error("Caught exception:", e);
                    throw new RuntimeException(e);
                  }
                });
              }
              // ---

              if (LOG.isDebugEnabled()) {
                LOG.debug("Committing result");
                sentenceWriter.commit();
              }
            }
          }
        }

        if (termTable != null) {
          scoringType[0] = "terms";
          // query for data
          final String querySQL = "select " +
              TermScoringTable.Fields.ID + ", " +
              TermScoringTable.Fields.TERM +
              " from " + TermScoringTable.TABLE_NAME +
              " where " + TermScoringTable.Fields.LANG + "='" + langName +
              "' and " + TermsTable.FieldsOptional.IPC +
              (includeIPC ?
                  " like '" + ipcName + "%'" : // filter by ipc
                  " is null;"); // ipc must not be present
          if (LOG.isDebugEnabled()) {
            LOG.debug("querySQL: {}", querySQL);
          }
          stmt.execute(querySQL);
          final ResultSet rs = stmt.getResultSet();

          try (final BM25TermScoringResultTable.Writer termWriter =
                   termTable.getWriter(scoringDb.getConnection())) {
            currentItemCount.set(0);
            while (rs.next()) {
              final Integer termId = rs.getInt(1);
              final String term = rs.getString(2);

              if (term == null) {
                throw new IllegalStateException("Term was null.");
              }

              @SuppressWarnings("ObjectAllocationInLoop")
              final TableFieldContent tfc =
                  new TableFieldContent(termTable);

              tfc.setValue(
                  BM25TermScoringResultTable.Fields.TERM_REF, termId);
              tfc.setValue(
                  BM25TermScoringResultTable.Fields.Q_FIELDS, qFields);

              if (includeIPC) {
                tfc.setValue(BM25TermScoringResultTable.Fields.Q_IPC, ipcName);
              }

              currentItemCount.incrementAndGet();

              // ---
              final BytesRefArray queryTerms =
                  QueryUtils.tokenizeQuery(term, analyzer, dataProv);
              // check query term extraction result
              if (queryTerms.size() <= 0) {
                LOG.warn(
                    "Tokenized query ist empty! Skipping B25+ score. q=[{}]",
                    term);
                tfc.setValue(
                    BM25TermScoringResultTable.Fields.DOC_ID, "-1");
                tfc.setValue(
                    BM25TermScoringResultTable.Fields.SCORE, "-1");
                // write result
                termWriter.addContent(tfc, false);
              } else {
                final DocIdSet feedbackDocIds = fbProv.query(queryTerms).get();
                StreamUtils.stream(feedbackDocIds).forEach(docId -> {
                  final double score =
                      bm25Calc.score(docId, queryTerms).doubleValue();
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("BM25+ q=[{}] docid={} score={}",
                        term, docId, score);
                  }
                  tfc.setValue(
                      BM25TermScoringResultTable.Fields.DOC_ID, docId);
                  tfc.setValue(
                      BM25TermScoringResultTable.Fields.SCORE, score);
                  // write result
                  try {
                    termWriter.addContent(tfc, false);
                  } catch (final SQLException e) {
                    LOG.error("Caught exception!", e);
                    throw new RuntimeException(e);
                  }
                });
              }
              // ---

              if (LOG.isDebugEnabled()) {
                LOG.debug("Committing result");
                termWriter.commit();
              }
            }
          }
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
    private static final Logger LOG = LoggerFactory.getLogger(Params.class);

    /**
     * Target database file file.
     */
    @Option(name = "-scoredb", metaVar = "<database file>", required = true,
        usage = "Source & target SQLite database with scoring data. " +
            "Must contain scoring terms and no scoring sentences.")
    File dbScoring;

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

    @Option(name = "-amount", required = false,
        usage = "Amount of results to query. Defaults to 10.")
    int amount = 10;

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
      // check amount value
      if (this.amount <= 0) {
        throw new IllegalArgumentException("Amount value must be >0.");
      }

      // check for database
      if (!this.dbScoring.exists() || !this.dbScoring.isFile()) {
        throw new IllegalStateException(
            "Required database " + this.dbScoring +
                " does not exist or is a not a database file.");
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
