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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import de.unihildesheim.iw.fiz.models.Patent;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import de.unihildesheim.iw.lucene.document.FeedbackQuery;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.scoringData.ScoringDataDB;
import de.unihildesheim.iw.storage.sql.scoringData.SentenceScoringTable;
import de.unihildesheim.iw.storage.sql.scoringData.TermScoringTable;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.Search.Builder;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class ScoringTermSentenceExtractor
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ScoringTermSentenceExtractor.class);
  /**
   * Regular expression matching some control characters that will get removed
   * before extracting sentences.
   */
  @SuppressWarnings("HardcodedLineSeparator")
  private static final Pattern RX_CONTROL_CHARS =
      Pattern.compile("(\\t|\\r|\\n)");
  /**
   * Regular expression matching whitespace characters.
   */
  private static final Pattern RX_WHITESPACE = Pattern.compile("\\s+");
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Client to ElasticSearch instance.
   */
  private JestClient client;
  /**
   * Compares two sentences by roughly estimating the number of words.
   */
  private final Comparator<String> sentenceComparator =
      new SentenceComparator();
  /**
   * Analyzer for query tokenizing.
   */
  private Analyzer analyzer;
  /**
   * Sentence detector instance to split a result into sentences.
   */
  private SentenceDetector sentDec;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ScoringTermSentenceExtractor() {
    super("Collect sentences for scoring terms.",
        "Collect sentences for terms stored in a scoring database.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws Exception Thrown on low-level i/o-errors
   */
  public static void main(@NotNull final String... args)
      throws Exception {
    new ScoringTermSentenceExtractor().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws Exception Thrown on low-level i/o-errors
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
      if (!scoringDb.hasTable(TermScoringTable.TABLE_NAME)) {
        throw new IllegalStateException(
            "The database is missing the required table '" +
                TermScoringTable.TABLE_NAME + "'.");
      }
      if (scoringDb.hasTable(SentenceScoringTable.TABLE_NAME)) {
        throw new IllegalStateException(
            "A table with the name '" + SentenceScoringTable.TABLE_NAME +
                "' already exists in the database.");
      }

      final SentenceScoringTable sentenceTable = new SentenceScoringTable();
      final MetaTable metaTable = new MetaTable();
      scoringDb.createTables(metaTable, sentenceTable);

      // write meta-data
      try (final MetaTable.Writer metaWriter =
               new MetaTable.Writer(scoringDb.getConnection())) {
        metaWriter.addContent(new TableFieldContent(metaTable)
            .setValue(MetaTable.Fields.TABLE_NAME, sentenceTable.getName())
            .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
      }

      // setup ES REST client
      final JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(
          new HttpClientConfig.Builder(ES_CONF.URL)
              .multiThreaded(true).build());
      this.client = factory.getObject();

      // lucene analyzer
      this.analyzer = LanguageBasedAnalyzers.createInstance(
          this.cliParams.language, CharArraySet.EMPTY_SET);

      // sentence detector (NLP)
      InputStream modelIn = null;
      try {
        // Loading sentence detection model
        modelIn = Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("nlpModels/" +
                this.cliParams.language + "-sent.bin");
        final SentenceModel sentenceModel = new SentenceModel(modelIn);
        modelIn.close();

        this.sentDec = new SentenceDetectorME(sentenceModel);
      } finally {
        if (modelIn != null) {
          modelIn.close();
        }
      }

      processTerms(scoringDb, sentenceTable);

      // check results
      final long termCount = scoringDb
          .getNumberOfRows(TermScoringTable.TABLE_NAME);
      final long sentenceCount = scoringDb
          .getNumberOfRows(SentenceScoringTable.TABLE_NAME);

      LOG.info("Checking results.");
      if (termCount != sentenceCount) {
        final String msg = "Number of terms and retrieved " +
            "sentences do not match. terms=" + termCount +
            " sentences=" + sentenceCount;
        LOG.error(msg);
        throw new IllegalStateException(msg);
      } else {
        LOG.info("Ok. terms={} sentences={}", termCount, sentenceCount);
      }
    }
  }

  /**
   * Iterate through all terms of the {@link TermScoringTable}.
   *
   * @param scoringDb Scoring data database
   * @param sentenceTable Table containing scoring sentences
   * @throws Exception Thrown on low-level ElasticSearch and Lucene errors
   */
  @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
  private void processTerms(
      @NotNull final ScoringDataDB scoringDb,
      @NotNull final SentenceScoringTable sentenceTable)
      throws Exception {
    // normalize some parameters
    final String langName = this.cliParams.language.toString();

    // query for terms
    final String querySQL = "select id, term, field from " +
        TermScoringTable.TABLE_NAME + " where lang='" + langName + "';";

    // lucene searcher
    final IndexSearcher searcher = IndexUtils.getSearcher(
        this.cliParams.idxReader);

    try (final SentenceScoringTable.Writer sentenceWriter =
             new SentenceScoringTable.Writer(scoringDb.getConnection())) {
      final Statement stmt = scoringDb.getConnection().createStatement();
      stmt.execute(querySQL);
      final ResultSet rs = stmt.getResultSet();

      long rowCount = scoringDb
          .getNumberOfRows(SentenceScoringTable.TABLE_NAME);
      long newRowCount = 0l;

      Statement insertStmt;
      SQLWarning insertWarn;

      while (rs.next()) {
        final Integer termId = rs.getInt(1);
        final String term = rs.getString(2);
        final String field = rs.getString(3);

        if (term == null) {
          throw new IllegalStateException("Term was null.");
        }
        if (field == null) {
          throw new IllegalStateException("Field was null.");
        }

        final List<String> refs = getMatchingDocs(
            this.cliParams.idxReader, searcher, term, field);
        final String sentence = getRandomSentence(refs, langName, field, term);
        if (sentence.isEmpty()) {
          throw new IllegalStateException("Empty sentence.");
        }

        LOG.debug("SENTENCE: {}\n  CP:{} t:{}", sentence,
            sentence.codePointCount(0, sentence.length()), term);

        @SuppressWarnings("ObjectAllocationInLoop")
        final TableFieldContent tfc = new TableFieldContent(sentenceTable);
        tfc.setValue(SentenceScoringTable.Fields.LANG, langName);
        tfc.setValue(SentenceScoringTable.Fields.TERM_REF, termId);
        tfc.setValue(SentenceScoringTable.Fields.SENTENCE, sentence);
        // write result
        insertStmt = sentenceWriter.addContent(tfc, false);
        insertWarn = insertStmt.getWarnings();

        newRowCount = scoringDb.getNumberOfRows(
            SentenceScoringTable.TABLE_NAME);
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

  /**
   * Queries the Lucene index for documents matching a single term.
   *
   * @param reader Index reader
   * @param searcher Index searcher
   * @param q Query term
   * @param fld Field to query
   * @return List of patent-reference ids from matched documents
   * @throws IOException Thrown on low-level I/O errors
   */
  private static List<String> getMatchingDocs(
      @NotNull final IndexReader reader,
      @NotNull final IndexSearcher searcher,
      @NotNull final String q,
      @NotNull final String fld)
      throws IOException {
    return StreamUtils.stream(
        FeedbackQuery.getMax(searcher, new TermQuery(new Term(fld, q)), 500))
        .mapToObj(id -> {
          try {
            return reader.document(id, Collections.singleton(
                LUCENE_CONF.FLD_PAT_ID));
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .map(doc -> doc.get(LUCENE_CONF.FLD_PAT_ID))
        .collect(Collectors.toList());
  }

  /**
   * Extract a random sentence from the remote ElasticSearch instance by using a
   * list of patent (id-)references.
   *
   * @param refs References of matching patents
   * @param lang Language to extract from
   * @param field Document field to extract from
   * @param term Term the sentence must contain
   * @return A random sentence picked from a list of matching sentences.
   * @throws Exception Trown on low-level ElasticSearch errors
   */
  private String getRandomSentence(
      @NotNull final List<String> refs,
      @NotNull final CharSequence lang,
      @NotNull final String field,
      @NotNull final String term)
      throws Exception {
    final String ref;
    if (refs.isEmpty()) {
      LOG.warn("No refs! l={} f={} t={}", lang, field, term);
      return "";
    } else if (refs.size() > 1) {
      ref = refs.get(RandomValue.getInteger(0, refs.size() - 1));
    } else {
      ref = refs.get(0);
    }

    final CharSequence lng = StringUtils.upperCase(lang);
    String sentence = "";

    final String fld;
    if ("claims".equalsIgnoreCase(field)) {
      fld = ES_CONF.FLD_CLAIM_PREFIX + lng;
    } else if ("detd".equalsIgnoreCase(field)) {
      fld = ES_CONF.FLD_DESC;
    } else {
      throw new IllegalArgumentException("Unknown field: '" + field + "'.");
    }

    // elastic search query
    @SuppressWarnings("HardcodedLineSeparator")
    final String esQuery =
        "{\n" +
            // single field we're interested in
            "  \"fields\": [\"" + fld + "\"],\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [{\n" +
            "        \"term\": {\n" +
            // field containing a patent identifier
            "          \"patent.PN\": \"" + ref + "\"\n" +
            "        }\n" +
            "      }]\n" +
            "    }\n" +
            "  }\n" +
            '}';

    final Search search = new Builder(esQuery)
        // index to query
        .addIndex(ES_CONF.INDEX)
            // document type to retrieve
        .addType(ES_CONF.DOC_TYPE)
        .build();

//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Term: {}", term);
//      LOG.debug("ESquery: {}", esQuery);
//    }

    // initialize the scroll search
    final JestResult result = ESUtils.runRequest(this.client, search);

//    if (LOG.isDebugEnabled()) {
//      LOG.debug("ESQueryResult: {}", result.getJsonString());
//    }

    if (result.isSucceeded()) {
      final JsonArray hits = result.getJsonObject()
          .getAsJsonObject("hits")
          .getAsJsonArray("hits");

      if (hits.size() == 1) {
        final JsonObject json = hits.get(0).getAsJsonObject();
        if (json.has("fields")) {
          final JsonObject jHits = json.getAsJsonObject("fields");
          if (jHits.has(fld)) {
            if ("claims".equalsIgnoreCase(field)) {
              sentence = Patent.joinJsonArray(jHits.getAsJsonArray(fld));
            } else if ("detd".equalsIgnoreCase(field)) {
              sentence = Patent.joinJsonArray(
                  jHits.getAsJsonArray(ES_CONF.FLD_DESC));
            }
          } else {
            LOG.error("Required field {} not found.", fld);
          }
        } else {
          LOG.error("No hit fields returned.");
        }
      } else {
        LOG.error("Expected 1 hit, got {}.", hits.size());
      }
    } else {
      LOG.error("Initial request failed. {}", result.getErrorMessage());
    }

    if (sentence.isEmpty()) {
      return sentence;
    } else {
      return pickSentence(analyzeSentence(sentence, term));
    }
  }

  /**
   * Splits a block of text into single sentences returning only those
   * containing a given term.
   *
   * @param content Text content.
   * @param term Term that must be contained in a matching sentence.
   * @return List of sentences containing the given term
   */
  private List<String> analyzeSentence(
      @NotNull final CharSequence content,
      @NotNull final CharSequence term)
      throws IOException {
    final String[] sentences = this.sentDec.sentDetect(
        RX_WHITESPACE.matcher(
            RX_CONTROL_CHARS.matcher(
                StringUtils.lowerCase(content)
            ).replaceAll(" ")
        ).replaceAll(" ")
    );

    final List<String> matchingSentences = new ArrayList<>(50);

    for (final String s : sentences) {
//      LOG.debug("Sentence: {}", s);
      QueryUtils.tokenizeQueryString(s, this.analyzer).stream()
          .filter(l -> l.contains(term))
          .findFirst()
          .ifPresent(t -> {
//            if (LOG.isDebugEnabled()) {
//              LOG.debug("T={} C={} S={}", term, t, s);
//            }
            matchingSentences.add(s);
          });
    }
    return matchingSentences;
  }

  /**
   * Pick a random sentence from a list of sentences.
   *
   * @param sentences Sentences to pick from
   * @return A single random sentence from the given list
   */
  private String pickSentence(final List<String> sentences) {
    sentences.sort(this.sentenceComparator);

    final int items = sentences.size();

    if (items == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("sentence: none");
      }
      return "";
    } else if (items == 1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("sentence: single");
      }
      return sentences.get(0);
    } else if (items <= 3) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("sentence: 1 of 3");
      }
      return sentences.get(RandomValue.getInteger(0, items - 1));
    } else {
      // get sentences from the upper thirds only
      final int lowerBound = items - (int) ((double) items / 3.5);
      if (LOG.isDebugEnabled()) {
        LOG.debug("sentence: pick {}->{}-{}", items, lowerBound, items);
      }
      return sentences.get(RandomValue.getInteger(lowerBound, items - 1));
    }
  }

  /**
   * Compares two sentences by roughly estimating the number of words.
   */
  private static class SentenceComparator
      implements Comparator<String>, Serializable {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -2378675043888177483L;

    /**
     * Empty constructor.
     */
    SentenceComparator() {
      // empty
    }

    @Override
    public int compare(final String o1, final String o2) {
      return Integer.compare(
          StringUtils.estimatedWordCount(o1),
          StringUtils.estimatedWordCount(o2));
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
    Language language;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    IndexReader idxReader;
    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    private Directory luceneDir;

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
