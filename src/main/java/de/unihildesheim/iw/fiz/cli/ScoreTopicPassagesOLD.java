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
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider.Builder;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryParserType;
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
import de.unihildesheim.iw.storage.xml.topics.PassagesListEntry;
import de.unihildesheim.iw.storage.xml.topics.ScoreEntry;
import de.unihildesheim.iw.storage.xml.topics.TopicsXML;
import de.unihildesheim.iw.storage.xml.topics.TopicsXML.MetaTags;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Reads a {@link TopicsXML} file and scores the extracted passages.
 *
 * @author Jens Bertram
 */
public final class ScoreTopicPassagesOLD
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ScoreTopicPassagesOLD.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Provides access to the topicsReader file.
   */
  private TopicsXML topicsXML;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ScoreTopicPassagesOLD() {
    super("Scores passages from claims.",
        "Scores passages extracted from CLEF-IP documents.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws DataProviderException Thrown if creating the {@link
   * IndexDataProvider} fails
   */
  public static void main(final String... args)
      throws DataProviderException {
    new ScoreTopicPassagesOLD().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   */
  private void runMain(final String... args) {
    final CmdLineParser parser = new CmdLineParser(this.cliParams);
    try {
      parser.parseArgument(args);
    } catch (final CmdLineException ex) {
      if (this.cliParams.listScorers) {
        printKnownScorers();
        System.exit(0);
      } else if (this.cliParams.listQueryParsers) {
        printKnownQParsers();
        System.exit(0);
      }
    }

    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    // init source file
    try {
      this.topicsXML = new TopicsXML(this.cliParams.sourceFile);
      this.topicsXML.setMeta(MetaTags.TIMESTAMP,
          new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss").toString());
      this.topicsXML.setMeta(MetaTags.IDX_FIELDS,
          StringUtils.join(this.cliParams.getFields(), " "));
    } catch (final JAXBException e) {
      LOG.error("Failed to load topics file.", e);
      System.exit(-1);
    }

    if (!scoreByLanguage()) {
      System.exit(-1);
    }
  }

  /**
   * Print a list of known scorer implementations.
   */
  static void printKnownScorers() {
    System.out.println("Scorers:");
    for (final ScorerType st : ScorerType.values()) {
      System.out.println(" * " + st.name() + ": " + st);
    }
  }

  /**
   * Print a list of known query parsers.
   */
  private static void printKnownQParsers() {
    System.out.println("Query parsers:");
    for (final QueryParserType qpt : QueryParserType.values()) {
      System.out.println(" * " + qpt.name() + ": " + qpt);
    }
  }

  /**
   * Scores a list of passages sorted by language. The languages to score are
   * set by commandline parameters {@link Params#lang}. <br> Calls {@link
   * #scoreByLanguage(String)} for each language.
   *
   * @return True, if scoring was successful
   */
  private boolean scoreByLanguage() {
    // error flag
    boolean succeeded;
    // check for single language
    LOG.info("Processing language '{}'.",
        this.cliParams.lang);
    try {
      succeeded = scoreByLanguage(this.cliParams.lang);
    } catch (IOException | BuildableException e) {
      LOG.error("Failed to score language '{}'.", this.cliParams.lang, e);
      succeeded = false;
    }
    try {
      this.topicsXML.writeToFile(this.cliParams.targetFile, true);
    } catch (final JAXBException e) {
      LOG.error("Failed to write scoring results.", e);
      succeeded = false;
    }
    return succeeded;
  }

  /**
   * Runs the scoring for a single language.
   *
   * @param lang Language to score. Uses a two-char language code.
   * @return True, if scoring was successful
   * @throws IOException Thrown on low-level i/o-errors
   * @throws BuildableException Thrown, if building the instance has failed
   */
  private boolean scoreByLanguage(final String lang)
      throws IOException, BuildableException {
    // error flag
    boolean succeeded = true;
    // get an analyzer for the target language
    final Analyzer analyzer;
    // check, if we have an analyzer
    if (!LanguageBasedAnalyzers.hasAnalyzer(lang)) {
      throw new IllegalArgumentException(
          "No analyzer for language '" + lang + "'.");
    }

    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}", lang,
        this.cliParams.getFields());

    final DirectoryReader reader = DirectoryReader.open(
        FSDirectory.open(this.cliParams.idxDir.toPath()));
    final FilteredDirectoryReader idxReader = new FilteredDirectoryReader
        .Builder(reader)
        .fields(this.cliParams.getFields())
        .build();

    try (final FDRIndexDataProvider dataProv = new Builder()
        .indexReader(idxReader)
        .build()) {
      analyzer = LanguageBasedAnalyzers.createInstance(
          LanguageBasedAnalyzers.getLanguage(lang));

      // build a list of scorers to use
      final Collection<Tuple2<AbstractCSCBuilder, Configuration>> scorer;

      // choose which scorers to use
      if (this.cliParams.scorerType == null) {
        // use all available scorers
        scorer = new ArrayList<>(ScorerType.values().length);
        for (final ScorerType st : ScorerType.values()) {
          scorer.add(getScorer(st, dataProv, idxReader, analyzer));
        }
        LOG.info("Using all available scorers.");
      } else {
        // Single scorer instance
        scorer = Collections.singletonList(
            getScorer(this.cliParams.scorerType, dataProv, idxReader,
                analyzer));
        LOG.info("Using only scorer: {}", this.cliParams.scorerType);
      }

      final TimeMeasure runTime = new TimeMeasure().start();

      // iterate over all scorers
      for (final Tuple2<AbstractCSCBuilder, Configuration> scorerT2 : scorer) {
        // scorer is auto-closable
        try {
          final ClarityScoreCalculation csc = scorerT2.a.build();
          final Configuration cscConf = scorerT2.b;

          this.topicsXML.addScorer(csc, cscConf);

          // get all passages for the current language
          final List<PassagesListEntry> passages =
              this.topicsXML.getPassages(lang);
          int passageCounter;
          ClarityScoreResult result;
          passageCounter = 0;

          // iterate over all passages using the current scorer
          for (final PassagesListEntry p : passages) {
            LOG.info("Scoring {} passages for language {}, scorer {}.",
                passages.size(), lang, csc.getIdentifier());
            try {
              //noinspection HardcodedFileSeparator
              LOG.info("Scoring [{}/{}], language {}, scorer {}. Runtime: {}",
                  ++passageCounter,
                  passages.size(), lang, csc.getIdentifier(),
                  runTime.getTimeString());
              result = csc.calculateClarity(p.getContent());
              @SuppressWarnings("ObjectAllocationInLoop")
              final ScoreEntry scoreEntry = new ScoreEntry();
              scoreEntry.setImpl(csc.getIdentifier());
              scoreEntry.setScore(result.getScore());
              scoreEntry.setEmpty(result.isEmpty());
//              scoreEntry.setResult(result.getXml());

              p.getScore().add(scoreEntry);
              LOG.info("Score {}: {}", csc.getIdentifier(), result.getScore());
            } catch (final ClarityScoreCalculationException e) {
              LOG.error("Clarity score calculation ({}) failed.",
                  csc.getIdentifier(), e);
              succeeded = false;
            }
          }
        } catch (final Exception e) {
          LOG.error("Error running scorer.", e);
          succeeded = false;
        }
      }
      LOG.info("Finished after {}.", runTime.stop().getTimeString());
    }
    return succeeded;
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
     * Source file containing extracted claims.
     */
    @Option(name = "-i", metaVar = "FILE", required = true,
        usage = "Input file containing extracted passages")
    File sourceFile;

    /**
     * Target file file for writing scored claims.
     */
    @Option(name = "-o", metaVar = "FILE", required = true,
        usage = "Output file for writing scored passages")
    File targetFile;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * Document-fields to query.
     */
    @Option(name = "-fields", metaVar = "list", required = true,
        handler = StringArrayOptionHandler.class,
        usage = "List of document fields separated by spaces to query. ")
    String[] fields;

    /**
     * Single languages.
     */
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process only the defined language.")
    String lang;

    /**
     * Single languages.
     */
    @Nullable
    @SuppressWarnings("PackageVisibleField")
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
     * List known scorers.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-scorers", usage = "List known scorers",
        required = false)
    boolean listScorers;

    /**
     * List known query parsers.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-qparsers", usage = "List known query parsers",
        required = false)
    boolean listQueryParsers;

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    Collection<String> docFields;

    Collection<String> getFields() {
      return this.docFields;
    }

    /**
     * Check, if the defined files and directories are available.
     */
    void check() {
      this.docFields = new HashSet<>(this.fields.length);
      Collections.addAll(this.docFields, this.fields);
      if (this.targetFile.exists()) {
        LOG.error("Target file '" + this.targetFile + "' already exist.");
        System.exit(-1);
      }
      if (!this.sourceFile.exists()) {
        LOG.error("Source file '" + this.sourceFile + "' does not exist.");
        System.exit(-1);
      }
      if (!this.idxDir.exists()) {
        LOG.error("Index directory'" + this.idxDir + "' does not exist.");
        System.exit(-1);
      }
      if (this.scorer != null) {
        this.scorerType = ScorerType.getByName(this.scorer);
        if (this.scorerType == null) {
          LOG.error("Unknown scorer type '" + this.scorer + "'.");
          printKnownScorers();
          System.exit(-1);
        }
      }
    }
  }
}
