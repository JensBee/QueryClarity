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
import de.unihildesheim.iw.cli.CliCommon;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder.Feature;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import de.unihildesheim.iw.lucene.index.LuceneIndexDataProvider;
import de.unihildesheim.iw.lucene.index.LuceneIndexDataProvider.Builder;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ClarityScoreCalculationException;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculationBuilder;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreResult;
import de.unihildesheim.iw.lucene.scoring.clarity.DefaultClarityScore;
import de.unihildesheim.iw.lucene.scoring.clarity.DefaultClarityScoreConfiguration;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScore;
import de.unihildesheim.iw.lucene.scoring.clarity.ImprovedClarityScoreConfiguration;
import de.unihildesheim.iw.lucene.scoring.clarity.SimplifiedClarityScore;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.StopwordsFileReader;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.xml.TopicsXMLWriter;
import de.unihildesheim.iw.xml.elements.Language;
import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.elements.Passage.Score;
import de.unihildesheim.iw.xml.elements.ScoreType;
import de.unihildesheim.iw.xml.elements.TopicPassages;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads a {@link TopicPassages} file and scores the extracted passages.
 *
 * @author Jens Bertram
 */
public final class ScoreTopicPassages
    extends CliBase {

  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(ScoreTopicPassages.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Provides access to the topicsXML file.
   */
  private TopicsXMLWriter topicsXML;
  /**
   * Results writer.
   */
  private XmlResult xmlResult;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ScoreTopicPassages() {
    super("Scores passages from claims.",
        "Scores passages extracted from CLEF-IP documents.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws DataProviderException
   */
  public static void main(final String[] args)
      throws DataProviderException {
    new ScoreTopicPassages().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws DataProviderException
   */
  private void runMain(final String[] args)
      throws DataProviderException {
    final CmdLineParser parser = new CmdLineParser(this.cliParams);
    try {
      parser.parseArgument(args);
    } catch (final CmdLineException ex) {
      if (this.cliParams.listScorers) {
        printKnownScorers();
        System.exit(0);
      }
    }

    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    // init topicsXML file
    try {
      this.topicsXML = new TopicsXMLWriter(this.cliParams.sourceFile);
    } catch (final JAXBException e) {
      LOG.error("Failed to load topics file.", e);
      System.exit(-1);
    }

    // init results writer
    this.xmlResult = new XmlResult(this.topicsXML);
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
   * Scores a list of passages sorted by language. The languages to score are
   * set by commandline parameters {@link Params#lang}. <br> Calls {@link
   * #scoreByLanguage(String)} for each language.
   *
   * @return True, if scoring was successfull
   */
  private boolean scoreByLanguage()
      throws DataProviderException {
    // error flag
    boolean succeeded = true;
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
      this.topicsXML.writeResults(this.cliParams.targetFile, true);
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
   * @return True, if scoring was successfull
   * @throws IOException
   * @throws BuildableException
   * @throws DataProviderException
   */
  private boolean scoreByLanguage(final String lang)
      throws IOException, BuildableException, DataProviderException {
    // error flag
    boolean succeeded = true;
    // get an analyzer for the target language
    final Analyzer analyzer;
    // check, if we have an analyzer
    if (!LanguageBasedAnalyzers.hasAnalyzer(lang)) {
      throw new IllegalArgumentException(
          "No analyzer for language '" + lang + "'.");
    }

    final Set<String> sWords = CliCommon.getStopwords(lang,
        this.cliParams.stopFileFormat, this.cliParams.stopFilePattern);

    // suffix all fields by language
    final Set<String> langFields =
        new HashSet<>(this.cliParams.fields.length);
    Collections.addAll(langFields, this.cliParams.fields);

    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}", lang,
        langFields);

    try (final LuceneIndexDataProvider dataProv = new Builder()
        .documentFields(langFields)
        .indexPath(this.cliParams.idxDir.getCanonicalPath())
        .stopwords(sWords)
        .setFeature(Feature.COMMON_TERM_THRESHOLD,
            Double.toString(this.cliParams.ctTreshold))
        .build()) {

      analyzer = LanguageBasedAnalyzers.createInstance(LanguageBasedAnalyzers
          .getLanguage(lang), dataProv);

      // reader to access the Lucene index
      final IndexReader idxReader =
          IndexUtils.openReader(this.cliParams.idxDir);
      // name of the db cache to create/load
      final String cacheName = this.cliParams.prefix + "_" + lang;

      // build a list of scorers to use
      final Collection<Tuple2<? extends
          ClarityScoreCalculationBuilder, ? extends Configuration>> scorer;

      // choose which scorers to use
      if (this.cliParams.scorerType == null) {
        // use all available scorers
        scorer = new ArrayList<>(ScorerType.values().length);
        for (final ScorerType st : ScorerType.values()) {
          scorer.add(getScorer(st, dataProv, cacheName,
              this.cliParams.dataDir.getCanonicalPath(), idxReader, analyzer));
        }
        LOG.info("Using all available scorers.");
      } else {
        // Single scorer instance
        scorer = Collections
            .<Tuple2<? extends
                ClarityScoreCalculationBuilder,
                ? extends Configuration>>singletonList(
                getScorer(this.cliParams.scorerType, dataProv, cacheName,
                    this.cliParams.dataDir.getCanonicalPath(), idxReader,
                    analyzer));
        LOG.info("Using only scorer: {}", this.cliParams.scorerType);
      }

      final TimeMeasure runTime = new TimeMeasure().start();

      // iterate over all scorers
      for (final Tuple2<? extends ClarityScoreCalculationBuilder,
          ? extends Configuration> scorerT2 : scorer) {
        // scorer is auto-closable
        try (final ClarityScoreCalculation csc = scorerT2.a.build()) {
          final Configuration cscConf = scorerT2.b;

          this.xmlResult.addScoreType(csc, cscConf);
          this.xmlResult.addLanguageStopwords(lang, sWords);

          // get all passages for the current language
          final List<Passage> passages = this.topicsXML.getPassages(lang);
          int passageCounter;
          ClarityScoreResult result;
          passageCounter = 0;

          // iterate over all passages using the current scorer
          for (final Passage p : passages) {
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
              final Score score = new Score(csc.getIdentifier(),
                  result.getScore(), result.isEmpty());
              score.setResult(result.getXml());

              p.getScores().add(score);
              LOG.info("Score {}: {}", csc.getIdentifier(), result.getScore());
            } catch (final ClarityScoreCalculationException e) {
              LOG.error("Clarity score calculation ({}) failed.",
                  csc.getIdentifier(), e);
              succeeded = false;
            }

            //LOG.debug("Updating results file");
            //this.topicsXML.writeResults(this.cliParams.targetFile, true);
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
   * @param cacheName Name of a DataProvider cache file to load
   * @param dataPath Path to store DataProvider working files
   * @param idxReader Lucene index reader
   * @param analyzer Lucene analyzer
   * @return Tuple containing a DataProvider instance and a configuration object
   * for the instance
   * @throws IOException
   */
  private Tuple2<? extends ClarityScoreCalculationBuilder,
      ? extends Configuration>
  getScorer(final ScorerType st,
      final IndexDataProvider dataProv,
      final String cacheName,
      final String dataPath,
      final IndexReader idxReader,
      final Analyzer analyzer)
      throws IOException {
    Tuple2<? extends ClarityScoreCalculationBuilder, ? extends Configuration>
        resTuple = null;
    switch (st) {
      case DCS:
        final DefaultClarityScoreConfiguration dcsc = new
            DefaultClarityScoreConfiguration();
        final ClarityScoreCalculationBuilder dcs =
            (ClarityScoreCalculationBuilder) new DefaultClarityScore.Builder()
                .loadOrCreateCache(cacheName)
                .dataPath(dataPath)
                .indexDataProvider(dataProv)
                .indexReader(idxReader)
                .configuration(dcsc)
                .analyzer(analyzer);
        resTuple = Tuple.tuple2(dcs, dcsc);
        break;
      case ICS:
        final ImprovedClarityScoreConfiguration icsc = new
            ImprovedClarityScoreConfiguration();
        final ClarityScoreCalculationBuilder ics =
            (ClarityScoreCalculationBuilder) new ImprovedClarityScore.Builder()
                .loadOrCreateCache(cacheName)
                .dataPath(dataPath)
                .indexDataProvider(dataProv)
                .indexReader(idxReader)
                .configuration(icsc)
                .analyzer(analyzer);
        resTuple = Tuple.tuple2(ics, icsc);
        break;
      case SCS:
        final ClarityScoreCalculationBuilder scs =
            (ClarityScoreCalculationBuilder) new SimplifiedClarityScore
                .Builder()
                .indexDataProvider(dataProv)
                .indexReader(idxReader)
                .analyzer(analyzer);
        resTuple = Tuple.tuple2(scs, new Configuration());
        break;
      default:
        // should never be reached
        throw new IllegalArgumentException("Unknown scorer type: " + st);
    }

    // check for custom configuration case
    //LOG.info("Using configuration: common-terms");
    //resTuple.a.feedbackProvider(new CommonTermsFeedbackProvider());
    return resTuple;
  }

  /**
   * Get a {@link ScorerType} by name.
   *
   * @param name Name to identify the scorer to get
   * @return Scorer instance, or {@code null} if none was found
   */
  static ScorerType getScorerTypeByName(final String name) {
    for (final ScorerType st : ScorerType.values()) {
      if (st.name().equalsIgnoreCase(name)) {
        return st;
      }
    }
    return null;
  }

  /**
   * Types of scorers available.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  enum ScorerType {
    /**
     * {@link DefaultClarityScore} scorer.
     */
    DCS("Default Clarity Score"),
    /**
     * {@link ImprovedClarityScore} scorer.
     */
    ICS("Improved Clarity Score"),
    /**
     * {@link SimplifiedClarityScore} scorer.
     */
    SCS("Simplified Clarity Score");

    /**
     * Current scorer name.
     */
    private final String name;

    /**
     * Create scorer type instance.
     *
     * @param sName Name of the scorer
     */
    ScorerType(final String sName) {
      this.name = sName;
    }

    /**
     * Get the name of the scorer.
     *
     * @return Scorer's name
     */
    public String toString() {
      return this.name;
    }
  }

  /**
   * Create the xml result.
   */
  private static final class XmlResult {
    /**
     * XML file writer instance.
     */
    private final TopicsXMLWriter topicsXML;

    /**
     * New instance based on a initialized XML writer.
     *
     * @param newTopicsXML Writer to use
     */
    XmlResult(final TopicsXMLWriter newTopicsXML) {
      this.topicsXML = newTopicsXML;
    }

    /**
     * Add a scorer to the results. The type and configuration of the scorer is
     * used to provide information in the result file.
     *
     * @param csc Scorer instance
     * @param cscConf Configuration used by instance
     */
    void addScoreType(final ClarityScoreCalculation csc,
        final Configuration cscConf) {
      final ScoreType xmlScoreType = new ScoreType();
      xmlScoreType.setImplementation(csc.getIdentifier());
      xmlScoreType.setConfiguration(cscConf.entryMap());
      this.topicsXML.addScoreType(xmlScoreType);
    }

    /**
     * Set the list of stopwords used for a language.
     *
     * @param lang Language identifier
     * @param sWords List of stopwords used
     */
    void addLanguageStopwords(final String lang,
        final Collection<String> sWords) {
      final Language xmlLanguage = new Language();
      xmlLanguage.setLanguage(lang);
      xmlLanguage.setStopwords(sWords);
      this.topicsXML.addStopwordsList(xmlLanguage);
    }
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Stopwords file format.
     */
    @SuppressWarnings({"PackageVisibleField", "FieldMayBeStatic"})
    @Option(name = "-stop-format", metaVar = "(plain|snowball)",
        required = false, depends = {"-stop"},
        usage = "Format of the stopwords file. 'plain' for a simple list of " +
            "each stopword per line. 'snowball' for a list of words and " +
            "comments starting with '|'. Defaults to 'plain'.")
    String stopFileFormat = "plain";
    /**
     * Source file containing extracted claims.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-i", metaVar = "FILE", required = true,
        usage = "Input file containing extracted passages")
    File sourceFile;
    /**
     * Target file file for writing scored claims.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-o", metaVar = "FILE", required = true,
        usage = "Output file for writing scored passages")
    File targetFile;
    /**
     * Directory containing the target Lucene index.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;
    /**
     * Directory for storing working data.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = CliParams.DATA_DIR_P, metaVar = CliParams.DATA_DIR_M,
        required = true, usage = CliParams.DATA_DIR_U)
    File dataDir;
    /**
     * Document-fields to query.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-fields", metaVar = "list", required = true,
        handler = StringArrayOptionHandler.class,
        usage = "List of document fields separated by spaces to query. ")
    String[] fields;

    /**
     * Prefix for cache data.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-prefix", metaVar = "name", required = true,
        usage = "Naming prefix for cached data files to load or create.")
    String prefix;
    /**
     * Pattern for stopwords files.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-stop", metaVar = "pattern", required = false,
        usage = "File naming pattern for stopword lists. " +
            "The pattern will be suffixed by '_<lang>.txt'. Stopword files " +
            "are expected to be UTF-8 encoded.")
    String stopFilePattern;
    /**
     * Single languages.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process only the defined language.")
    String lang;
    /**
     * Single languages.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-scorer", metaVar = "scorerName", required = false,
        usage = "Process only using the defined scorer.")
    String scorer;
    /**
     * Concrete instance of the provided {@link #scorer}. Only set, if {@link
     * #scorer} is provided and valid.
     */
    @SuppressWarnings("PackageVisibleField")
    ScorerType scorerType;
    /**
     * List known scorers.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-scorers", usage = "List known scorers",
        required = false)
    boolean listScorers;
    /**
     * Pre-defined configuration to use.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-ct-threshold",
        required = true,
        usage = "Common-terms threshold value")
    double ctTreshold;

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     */
    void check() {
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
      if (!this.dataDir.exists()) {
        LOG.info("Data directory'" + this.dataDir +
            "' does not exist and will be created.");
      }
      if (StopwordsFileReader.getFormatFromString(this.stopFileFormat) ==
          null) {
        LOG.error(
            "Unknown stopwords file format '" + this.stopFileFormat + "'.");
        System.exit(-1);
      }
      if (this.scorer != null) {
        this.scorerType = getScorerTypeByName(this.scorer);
        if (this.scorerType == null) {
          LOG.error("Unknown scorer type '" + this.scorer + "'.");
          printKnownScorers();
          System.exit(-1);
        }
      }
    }
  }

}
