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

import de.unihildesheim.iw.Buildable.BuildableException;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.clarity
    .AbstractClarityScoreCalculation.AbstractCSCBuilder;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation
    .ClarityScoreCalculationException;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreResult;
import de.unihildesheim.iw.lucene.scoring.clarity.DefaultClarityScore;
import de.unihildesheim.iw.lucene.scoring.clarity
    .DefaultClarityScoreConfiguration;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScore;
import de.unihildesheim.iw.lucene.scoring.clarity
    .ImprovedClarityScoreConfiguration;
import de.unihildesheim.iw.lucene.scoring.clarity.ScorerType;
import de.unihildesheim.iw.lucene.scoring.clarity.SimplifiedClarityScore;
import de.unihildesheim.iw.lucene.scoring.data.CommonTermsFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.scoringData.ScoringDataDB;
import de.unihildesheim.iw.storage.sql.scoringData.SentenceScoringResultTable;
import de.unihildesheim.iw.storage.sql.scoringData.SentenceScoringTable;
import de.unihildesheim.iw.storage.sql.scoringData.TermScoringResultTable;
import de.unihildesheim.iw.storage.sql.scoringData.TermScoringTable;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TaskObserver;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
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
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class Score
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(Score.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Analyzer for query tokenizing.
   */
  private Analyzer analyzer;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private Score() {
    super("Scores passages from claims.",
        "Scores passages extracted from CLEF-IP documents.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String... args)
      throws Exception {
    new Score().runMain(args);
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

    try (final ScoringDataDB scoringDb =
             new ScoringDataDB(this.cliParams.dbScoring)) {
      final boolean scoreTerms = scoringDb.hasTable(
          TermScoringTable.TABLE_NAME);
      final boolean scoreSentences = scoringDb.hasTable(
          SentenceScoringTable.TABLE_NAME);

      if (!scoreTerms && !scoreSentences) {
        throw new IllegalStateException(
            "Nothing to score. No terms & sentences found in database.");
      }

      final MetaTable metaTable = new MetaTable();
      TermScoringResultTable termTable = null;
      SentenceScoringResultTable sentenceTable = null;
      if (scoreTerms) {
        termTable = new TermScoringResultTable();
        scoringDb.createTables(termTable);
      }
      if (scoreSentences) {
        sentenceTable = new SentenceScoringResultTable();
        scoringDb.createTables(sentenceTable);
      }

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

        // lucene analyzer
        this.analyzer = LanguageBasedAnalyzers.createInstance(
            this.cliParams.language, CharArraySet.EMPTY_SET);

        runScoring(scoringDb, termTable, sentenceTable);
      }
    }
  }

  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  private void runScoring(
      @NotNull final ScoringDataDB scoringDb,
      @Nullable final TermScoringResultTable termTable,
      @Nullable final SentenceScoringResultTable sentenceTable)
      throws BuildableException, SQLException,
             ClarityScoreCalculationException {
    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}",
        this.cliParams.language, this.cliParams.docFields);

    final FilteredDirectoryReader idxReader = new FilteredDirectoryReader
        .Builder(this.cliParams.idxReader)
        .fields(this.cliParams.docFields)
        .build();

    try (final FDRIndexDataProvider dataProv = new FDRIndexDataProvider
        .Builder()
        .indexReader(idxReader)
        .build()) {

      // build a list of scorers to use
      final Collection<Tuple2<AbstractCSCBuilder, Configuration>> scorer;

      // choose which scorers to use
      if (this.cliParams.scorerType == null) {
        // use all available scorers
        scorer = new ArrayList<>(ScorerType.values().length);
        for (final ScorerType st : ScorerType.values()) {
          scorer.add(getScorer(st, dataProv, idxReader, this.analyzer));
        }
        LOG.info("Using all available scorers.");
      } else {
        // Single scorer instance
        scorer = Collections.singletonList(
            getScorer(this.cliParams.scorerType,
                dataProv, idxReader, this.analyzer));
        LOG.info("Using only scorer: {}", this.cliParams.scorerType);
      }

      // normalize some parameters
      final String langName = this.cliParams.language.toString();

      ClarityScoreResult result;
      String querySQL;
      final Statement stmt = scoringDb.getConnection().createStatement();
      long newRowCount;
      Statement insertStmt;
      SQLWarning insertWarn;

      final String[] stateMsg = {"none", "none"};
      
      try (TaskObserver obs = new TaskObserver(
          new TaskObserver.TaskObserverMessage() {
            @Override
            public void call(@NotNull final TimeMeasure tm) {
              LOG.info("Scorer {} is scoring {} (runtime: {}).",
                  stateMsg[0], stateMsg[1], tm.getTimeString());
            }
          })) {
        obs.start();
        // iterate over all scorers
        for (final Tuple2<AbstractCSCBuilder, Configuration> scorerT2 :
            scorer) {
          // scorer is auto-closable
          final ClarityScoreCalculation csc = scorerT2.a.build();
          //final Configuration cscConf = scorerT2.b;
          final String impl = csc.getIdentifier();
          stateMsg[0] = impl;

          if (termTable != null) {
            stateMsg[1] = "terms";
            // query for data
            querySQL = "select " +
                TermScoringTable.Fields.ID + ", " +
                TermScoringTable.Fields.TERM +
                " from " + TermScoringTable.TABLE_NAME +
                " where " + TermScoringTable.Fields.LANG + "='" + langName +
                "';";
            stmt.execute(querySQL);
            final ResultSet rs = stmt.getResultSet();

            long rowCount = scoringDb
                .getNumberOfRows(SentenceScoringTable.TABLE_NAME);

            try (final TermScoringResultTable.Writer termWriter =
                     termTable.getWriter(scoringDb.getConnection())) {
              while (rs.next()) {
                final Integer termId = rs.getInt(1);
                final String term = rs.getString(2);

                if (term == null) {
                  throw new IllegalStateException("Term was null.");
                }

                result = csc.calculateClarity(term);

                @SuppressWarnings("ObjectAllocationInLoop")
                final TableFieldContent tfc =
                    new TableFieldContent(termTable);
                tfc.setValue(TermScoringResultTable.Fields.TERM_REF, termId);
                tfc.setValue(TermScoringResultTable.Fields.IMPL, impl);
                tfc.setValue(TermScoringResultTable.Fields.IS_EMPTY,
                    result.isEmpty());
                tfc.setValue(TermScoringResultTable.Fields.SCORE,
                    result.getScore());

                // write result
                insertStmt = termWriter.addContent(tfc, false);
                insertWarn = insertStmt.getWarnings();

                newRowCount = scoringDb.getNumberOfRows(
                    TermScoringResultTable.TABLE_NAME);
                if (newRowCount <= rowCount) {
                  if (insertWarn != null) {
                    SQLWarning sqlWarn;
                    while ((sqlWarn = insertWarn.getNextWarning()) != null) {
                      LOG.error("SQL-warning: {}", sqlWarn.getMessage());
                    }
                  }
                  throw new IllegalStateException("Sentence row not written.");
                } else {
                  rowCount = newRowCount;
                }
              }
            }
          }

          if (sentenceTable != null) {
            stateMsg[1] = "sentences";
            // query for data
            querySQL = "select " +
                "s." + SentenceScoringTable.Fields.ID + ", " +
                "s." + SentenceScoringTable.Fields.SENTENCE +
                "t." + TermScoringTable.Fields.LANG +
                " from " + SentenceScoringTable.TABLE_NAME + " s" +
                " inner join " +
                TermScoringTable.TABLE_NAME + " t" +
                " on (s." + SentenceScoringTable.Fields.TERM_REF +
                " = t." + TermScoringTable.Fields.ID + ") " +
                " where t." + TermScoringTable.Fields.LANG + "='" +
                langName + "';";
            stmt.execute(querySQL);
            final ResultSet rs = stmt.getResultSet();

            long rowCount = scoringDb
                .getNumberOfRows(SentenceScoringTable.TABLE_NAME);

            try (final SentenceScoringResultTable.Writer sentenceWriter =
                     sentenceTable.getWriter(scoringDb.getConnection())) {
              while (rs.next()) {
                final Integer sentId = rs.getInt(1);
                final String sent = rs.getString(2);

                if (sent == null) {
                  throw new IllegalStateException("Sentence was null.");
                }

                result = csc.calculateClarity(sent);

                @SuppressWarnings("ObjectAllocationInLoop")
                final TableFieldContent tfc =
                    new TableFieldContent(sentenceTable);
                tfc.setValue(SentenceScoringResultTable.Fields.SENT_REF,
                    sentId);
                tfc.setValue(SentenceScoringResultTable.Fields.IMPL, impl);
                tfc.setValue(SentenceScoringResultTable.Fields.IS_EMPTY,
                    result.isEmpty());
                tfc.setValue(SentenceScoringResultTable.Fields.SCORE,
                    result.getScore());

                // write result
                insertStmt = sentenceWriter.addContent(tfc, false);
                insertWarn = insertStmt.getWarnings();

                newRowCount = scoringDb.getNumberOfRows(
                    SentenceScoringResultTable.TABLE_NAME);
                if (newRowCount <= rowCount) {
                  if (insertWarn != null) {
                    SQLWarning sqlWarn;
                    while ((sqlWarn = insertWarn.getNextWarning()) != null) {
                      LOG.error("SQL-warning: {}", sqlWarn.getMessage());
                    }
                  }
                  throw new IllegalStateException("Sentence row not written.");
                } else {
                  rowCount = newRowCount;
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Get a configured scorer.
   *
   * @param st Scorer type
   * @param dataProv DataProvider to access index data
   * @param idxReader Lucene index reader
   * @param analyzer Lucene analyzer
   * @return Tuple containing a DataProvider instance and a configuration object
   * for the instance
   */
  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  private static Tuple2<AbstractCSCBuilder, Configuration> getScorer(
      @NotNull final ScorerType st,
      @NotNull final IndexDataProvider dataProv,
      @NotNull final IndexReader idxReader,
      @NotNull final Analyzer analyzer) {
    final Tuple2<AbstractCSCBuilder, Configuration> resTuple;
    final FeedbackProvider fbProv = new CommonTermsFeedbackProvider();
    switch (st) {
      case DCS:
        final DefaultClarityScoreConfiguration dcsc = new
            DefaultClarityScoreConfiguration();
        final AbstractCSCBuilder dcs = new DefaultClarityScore.Builder()
            .indexDataProvider(dataProv)
            .indexReader(idxReader)
            .feedbackProvider(fbProv)
            .configuration(dcsc)
            .analyzer(analyzer);
        resTuple = Tuple.tuple2(dcs, dcsc);
        break;
      case ICS:
        final ImprovedClarityScoreConfiguration icsc = new
            ImprovedClarityScoreConfiguration();
        final AbstractCSCBuilder ics = new ImprovedClarityScore.Builder()
            .indexDataProvider(dataProv)
            .indexReader(idxReader)
            .feedbackProvider(fbProv)
            .configuration(icsc)
            .analyzer(analyzer);
        resTuple = Tuple.tuple2(ics, icsc);
        break;
      case SCS:
        final AbstractCSCBuilder scs = new SimplifiedClarityScore.Builder()
            .indexDataProvider(dataProv)
            .indexReader(idxReader)
            .analyzer(analyzer);
        resTuple = Tuple.tuple2(scs, new Configuration());
        break;
      default:
        // should never be reached
        throw new IllegalArgumentException("Unknown scorer type: " + st);
    }
    return resTuple;
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
    Language language;

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
     * Single languages.
     */
    @Nullable
    @Option(name = "-scorer", metaVar = "scorerName", required = false,
        usage = "Process only using the defined scorer.")
    String scorer;

    /**
     * Concrete instance of the provided {@link #scorer}. Only set, if {@link
     * #scorer} is provided and valid.
     */
    @SuppressWarnings("PackageVisibleField")
    @Nullable
    ScorerType scorerType;

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

      // scorer
      if (this.scorer != null) {
        this.scorerType = ScorerType.getByName(this.scorer);
        if (this.scorerType == null) {
          LOG.error("Unknown scorer type '" + this.scorer + "'.");
          System.exit(-1);
        }
      }
    }
  }
}
