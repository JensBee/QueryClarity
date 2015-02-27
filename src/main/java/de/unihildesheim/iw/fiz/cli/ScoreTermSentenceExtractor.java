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
import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.document.FeedbackQuery;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.query.TryExactTermsQuery;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.xml.TopicsXMLWriter;
import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.elements.PassagesGroup;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig.Builder;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class ScoreTermSentenceExtractor
    extends CliBase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ScoreTermSentenceExtractor.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Provides access to the topicsXML file.
   */
  private TopicsXMLWriter topicsXML;
  private TopicsXMLWriter topicsTargetXML;
  private Analyzer analyzer;
  private IndexSearcher searcher;
  private SentenceDetector sentDec;
  /**
   * Client to ElasticSearch instance.
   */
  private JestClient client;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ScoreTermSentenceExtractor() {
    super("Extract sentences based on termDump score terms.",
        "Extract content from remote ES instance by given term.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String[] args)
      throws Exception {
    new ScoreTermSentenceExtractor().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   */
  private void runMain(final String[] args)
      throws Exception {
    new CmdLineParser(this.cliParams);
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
    this.topicsTargetXML = new TopicsXMLWriter(this.cliParams.targetFile);

    // check, if we have an analyzer
    if (!LanguageBasedAnalyzers.hasAnalyzer(this.cliParams.lang)) {
      throw new IllegalArgumentException(
          "No analyzer for language '" + this.cliParams.lang + "'.");
    }
    this.analyzer = LanguageBasedAnalyzers.createInstance(LanguageBasedAnalyzers
            .getLanguage(this.cliParams.lang), LuceneDefaults.VERSION,
        CharArraySet.EMPTY_SET);

    // lucene searcher
    this.searcher = new IndexSearcher(this.cliParams.idxReader);

    // setup ES REST client
    final JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(new Builder(ES_CONF.URL)
        .multiThreaded(true).build());
    this.client = factory.getObject();

    // sentence detector (NLP)
    InputStream modelIn = null;
    try {
      // Loading sentence detection model
      modelIn = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("nlpModels/" + StringUtils.lowerCase(
              this.cliParams.lang) + "-sent.bin");
      final SentenceModel sentenceModel = new SentenceModel(modelIn);
      modelIn.close();

      this.sentDec = new SentenceDetectorME(sentenceModel);
    } finally {
      if (modelIn != null) {
        modelIn.close();
      }
    }

    processTerms();
    this.topicsTargetXML.writeResults(this.cliParams.targetFile, false);
  }

  enum Field {
    CLAIMS, DETD
  }

  private Field isValidField(final String fld) {
    for (final Field f : Field.values()) {
      if (f.name().equalsIgnoreCase(fld)) {
        return f;
      }
    }
    throw new IllegalArgumentException("Unknown field '" + fld + "'.");
  }

  private void processTerms()
      throws Exception {
    final String lang = this.cliParams.lang;

    final Collection<PassagesGroup> pGroups =
        this.topicsXML.getPassagesGroups();

    int passageCounter = 0;

    for (final PassagesGroup pg : pGroups) {
      final String field = pg.getSource().split(":")[0];
      final List<Passage> pl = pg.getPassages().stream()
          .filter(p -> lang.equals(StringUtils.lowerCase(p.getLanguage())))
          .collect(Collectors.toList());

      final PassagesGroup oPg = new PassagesGroup(pg.getSource());
      for (final Passage p : pl) {
        LOG.info("Extracting [{}/{}], language {}.",
            ++passageCounter, pl.size(), lang);
//        LOG.debug("Term: {} Fld: {}", p.getContent(), field);
        final List<String> refs = getMatchingDocs(p.getContent(), field);
//        LOG.debug("Refs: {}", refs);
        final String sentence =
            getRandomSentence(refs, lang, isValidField(field), p.getContent());
//        LOG.debug("RSent: {}", sentence);
        if (!sentence.isEmpty()) {
          oPg.getPassages().add(new Passage(lang, sentence));
        }
      }
      if (oPg.getPassages().isEmpty()) {
        LOG.warn("No content for source {}.", pg.getSource());
      } else {
        this.topicsTargetXML.getPassagesGroups().add(oPg);
      }
//      LOG.debug("==PG {}", this.topicsTargetXML.getPassagesGroups().size());
    }
  }

  private String getRandomSentence(final List<String> refs,
      final String lang, final Field f, final String term)
      throws Exception {

    throw new UnsupportedOperationException("Currently broken.");
//    final String ref;
//    if (refs.isEmpty()) {
//      LOG.warn("No refs! l={} f={} t={}", lang, f, term);
//      return "";
//    } else if (refs.size() > 1) {
//      ref = refs.get(RandomValue.getInteger(0, refs.size() - 1));
//    } else {
//      ref = refs.get(0);
//    }
//
//    final String lng = StringUtils.upperCase(lang);
//    String sentence = "";
//
//    final String fld;
//    switch (f) {
//      case CLAIMS:
//        // claim by language
//        fld = ES_CONF.FLD_CLAIM_PREFIX + lng;
//        break;
//      case DETD:
//        fld = ES_CONF.FLD_DESC;
//        break;
//      default:
//        // should never be reached
//        throw new IllegalArgumentException();
//    }
//
//    final Search search = new Search.Builder(new SearchSourceBuilder()
//        .field(fld)
//        .query(QueryBuilders.matchQuery(ES_CONF.FLD_PATREF, ref))
//        .toString())
//        // document type to retrieve
//        .addType(ES_CONF.DOC_TYPE)
//        .build();
//
//    // initialize the scroll search
//    final JestResult result = ESUtils.runRequest(this.client, search);
//
//    if (result.isSucceeded()) {
//      final JsonArray hits = result.getJsonObject()
//          .getAsJsonObject("hits")
//          .getAsJsonArray("hits");
//
//      if (hits.size() == 1) {
//        final JsonObject json = hits.get(0).getAsJsonObject();
//        if (json.has("fields")) {
//          final JsonObject jHits = json.getAsJsonObject("fields");
//          if (jHits.has(fld)) {
//            switch (f) {
//              case CLAIMS:
//                sentence = Patent.joinJsonArray(jHits.getAsJsonArray(fld));
//                break;
//              case DETD:
//                sentence = Patent.joinJsonArray(
//                    jHits.getAsJsonArray(ES_CONF.FLD_DESC));
//                break;
//            }
//          } else {
//            LOG.error("Required field {} not found.", fld);
//          }
//        } else {
//          LOG.error("No hit fields returned.");
//        }
//      } else {
//        LOG.error("Expected 1 hit, got {}.", hits.size());
//      }
//    } else {
//      LOG.error("Initial request failed. {}", result.getErrorMessage());
//    }
//
//    return pickSentence(analyzeSentence(sentence, term));
  }

  private String pickSentence(final List<String> sentences) {
    sentences.sort(new Comparator<String>() {
      @Override
      public int compare(final String o1, final String o2) {
        return Integer.compare(o1.split(" ").length, o2.split(" ").length);
      }
    });

    final int items = sentences.size();

    if (items == 0) {
      LOG.debug("sentence: none");
      return "";
    } else if (items == 1) {
      LOG.debug("sentence: single");
      return sentences.get(0);
    } else if (items <= 3) {
      LOG.debug("sentence: 1 of 3");
      return sentences.get(RandomValue.getInteger(0, items - 1));
    } else {
      // get sentences from the upper thirds only
      final int lowerBound = items - (int) ((double) items / 3.5);
      LOG.debug("sentence: pick {}->{}-{}", items, lowerBound, items);
      return sentences.get(RandomValue.getInteger(lowerBound, items - 1));
    }
  }

  private List<String> analyzeSentence(
      final String content, final String term) {
    final String[] sentences = this.sentDec.sentDetect(
        content
            .replaceAll("(\\t|\\r|\\n)", " ")
            .replaceAll("\\s+", " ")
    );

    final List<String> matchingSentences = new ArrayList(50);

    for (final String s : sentences) {
      QueryUtils.tokenizeQueryString(s, this.analyzer).stream()
          .filter(l -> l.contains(term))
          .findFirst()
          .ifPresent(t -> {
//            LOG.debug("T={} C={} S={}", term, t, s);
            matchingSentences.add(s);
          });
    }
    return matchingSentences;
  }

  private List<String> getMatchingDocs(final String q, final String fld)
      throws ParseException, IOException {
    return FeedbackQuery.getMinMax(
        this.searcher,
        new TryExactTermsQuery(this.analyzer, q, Collections.singleton(fld)),
        1, 500).stream()
        .map(id -> {
          try {
            return this.cliParams.idxReader.document(id,
                Collections.singleton(IndexBuilder.LUCENE_CONF.FLD_PAT_ID));
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .map(doc -> doc.get(IndexBuilder.LUCENE_CONF.FLD_PAT_ID))
        .collect(Collectors.toList());
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
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
     * Single language.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process for the defined language.")
    String lang;
    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    private Directory luceneDir;
    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    @SuppressWarnings("PackageVisibleField")
    IndexReader idxReader;

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     */
    void check()
        throws IOException {
      if (this.targetFile.exists()) {
        LOG.error("Target file '" + this.targetFile + "' already exist.");
        System.exit(-1);
      }
      if (!this.sourceFile.exists()) {
        LOG.error("Source file '" + this.sourceFile + "' does not exist.");
        System.exit(-1);
      }
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
    }
  }
}
