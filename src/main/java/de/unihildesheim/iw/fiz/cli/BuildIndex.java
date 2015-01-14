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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.fiz.Defaults.SRC_LANGUAGE;
import de.unihildesheim.iw.fiz.lucene.LanguageBasedAnalyzers;
import de.unihildesheim.iw.fiz.models.Patent;
import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.util.StopwordsFileReader;
import de.unihildesheim.iw.util.StopwordsFileReader.Format;
import de.unihildesheim.iw.util.StringUtils;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig.Builder;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * Builds a language specific Lucene index from all documents queried from a
 * remote documents repository.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class BuildIndex
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG = org.slf4j.LoggerFactory.getLogger(BuildIndex.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Client to ElasticSearch instance.
   */
  private JestClient client;
  /** Used to generate randomized delays for request throttling. */
  private final Random rand = new Random();

  /** ES settings. (TODO: make these external) */
  private static final class ES_CONF {
    // basic settings
    /** Name of the index to query. */
    private static final String INDEX = "epfull_repo";
    /** URL to reach the index. */
    private static final String URL = "http://t4p.fiz-karlsruhe.de:80";
    /** Type of document to query for. */
    private static final String DOC_TYPE = "patent";
    /** Number of results to get from each shard. */
    private static final int PAGE_SIZE = 150;
    /** How long to keep the scroll open. */
    private static final String SCROLL_KEEP = "15m";

    // document fields
    /** Field name containing the document id. */
    private static final String FLD_DOCID = "_id";
    /** Prefix name of the claims field. */
    private static final String FLD_CLAIM_PREFIX = "CLM";
    /** Field name holding the description. */
    private static final String FLD_DESC = "DETD";
    /** Field name holding the description language used. */
    private static final String FLD_DESC_LNG = "DETDL";

    /** How many times to retry a connection. */
    private static final int MAX_RETRY = 15;
  }

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private BuildIndex() {
    super("Create local term index.",
        "Create the local term index from the remote patents repository.");
  }

  /**
   * Runs a REST request against the ES instance. Optionally retrying the
   * request {@link ES_CONF#MAX_RETRY} times, if a request has timed out.
   * @param action Request action
   * @return Request result
   * @throws Exception Thrown on any error while performing the request
   */
  private JestResult runRequest(final Action action)
      throws Exception {
    int tries = 0;
    while (tries < ES_CONF.MAX_RETRY) {
      try {
        return this.client.execute(action);
      } catch (final SocketTimeoutException ex) {
        // connection timed out - retry after a short delay
        final int delay = (1 + this.rand.nextInt(10)) * 1000;
        LOG.warn("Timeout - retry ~{}..", delay);
        Thread.sleep((long) delay);
        tries++;
      }
    }
    // retries maxed out
    throw new RuntimeException("Giving up trying to connect after "+tries+" " +
        "retries.");
  }

  /**
   * Prepare a Lucene index for the given language. This will initialize a
   * language specific analyzer and creates the directories necessary to
   * contain the index.
   * @param lang Language
   * @return Writer targeting the language specific index
   * @throws IOException Thrown, if setting up the index target fails
   */
  private IndexWriter getIndexWriter(final SRC_LANGUAGE lang)
      throws IOException {
    // check, if we've an analyzer for the current language
    if (!LanguageBasedAnalyzers.hasAnalyzer(lang.toString())) {
      throw new IllegalArgumentException(
          "No analyzer for language '" + lang + "'.");
    }

    // get an analyzer for the target language
    final Set<String> sWords = getStopwords(lang.toString());
    final Analyzer analyzer = LanguageBasedAnalyzers.createInstance
        (LanguageBasedAnalyzers.getLanguage(lang.toString()),
            LuceneDefaults.VERSION, new CharArraySet(LuceneDefaults.VERSION,
                sWords, true));

    // create Lucene index in language specific sub-directory
    final File target = new File(this.cliParams.dataDir.getAbsolutePath() + File
        .separator + lang);
    Files.createDirectories(target.toPath());

    // Lucene index setup
    final Directory index = FSDirectory.open(target);
    final IndexWriterConfig config = new IndexWriterConfig(LuceneDefaults
        .VERSION, analyzer);

    return new IndexWriter(index, config);
  }

  /**
   * Index the documents for a specific language.
   * @param lang Language to index.
   * @throws Exception Thrown on REST request errors
   */
  private void indexByLanguage(final SRC_LANGUAGE lang)
      throws Exception {
    LOG.info("Creating index for: {}", lang);

    // claim by language
    final String fld_claim = ES_CONF.FLD_CLAIM_PREFIX + lang;

    // get all claims from patents in the specific language including
    // detailed technical description, if available
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
            FilterBuilders.orFilter(
                FilterBuilders.existsFilter(fld_claim),
                FilterBuilders.andFilter(
                    FilterBuilders.termFilter(ES_CONF.FLD_DESC_LNG,
                        lang.toString()),
                    FilterBuilders.existsFilter(ES_CONF.FLD_DESC)
                )
            )
    ));
    searchSourceBuilder.field(fld_claim); // claims
    searchSourceBuilder.field(ES_CONF.FLD_DOCID); // document id
    searchSourceBuilder.field(ES_CONF.FLD_DESC); // detailed description
    // detailed description language
    searchSourceBuilder.field(ES_CONF.FLD_DESC_LNG);

    // setup the search using scan & scroll
    final Search search = new Search.Builder(searchSourceBuilder.toString())
        // index to query
        .addIndex(ES_CONF.INDEX)
        // document type to retrieve
        .addType(ES_CONF.DOC_TYPE)
        // FIXME: using SearchType.SCAN does not work. ES instance requires
        // parameter to be a lower-cased string.
        .setParameter(Parameters.SEARCH_TYPE, "scan")//SearchType.SCAN)
        // hits per shard, each scroll
        .setParameter(Parameters.SIZE, ES_CONF.PAGE_SIZE)
        // keep scroll open for a specific time
        .setParameter(Parameters.SCROLL, ES_CONF.SCROLL_KEEP)
        .build();

    // initialize the scroll search
    JestResult result = runRequest(search); //this.client.execute(search);

    if (!result.isSucceeded()) {
      LOG.error("Initial request failed. {}", result.getErrorMessage());

    }

    JsonObject resultJson = result.getJsonObject();
    JsonArray hits = resultJson.getAsJsonObject("hits").getAsJsonArray("hits");

    String scrollId = resultJson.get("_scroll_id").getAsString();
    final String hitsTotal = resultJson.getAsJsonObject("hits")
        .get("total").toString();
    int currentResultSize = hits.size();

    LOG.debug("{} - hits:{}/{} scroll-id:{}", lang, currentResultSize,
        hitsTotal, scrollId);

    final IndexWriter writer = getIndexWriter(lang);
    indexResults(writer, lang, hits);

    // scroll through pages to gather all results
    int pageNumber = 1; // number of pages requested
    long dataCount = (long) currentResultSize; // number of items retrieved
    boolean retry = true; // retry poll request
    int delay; // throttle delay
    do {
      // retry a request if it has timed out
      retry = true;
      delay = (1 + this.rand.nextInt(10)) * 100;

      Thread.sleep((long) delay);
      result = runRequest(new SearchScroll.Builder(scrollId, ES_CONF.SCROLL_KEEP)
          .build());

      if (result.isSucceeded()) {
        // parse result set
        resultJson = result.getJsonObject();
        hits = resultJson.getAsJsonObject("hits").getAsJsonArray("hits");
        currentResultSize = hits.size();
        scrollId = resultJson.get("_scroll_id").getAsString();

        // index results
        indexResults(writer, lang, hits);

        dataCount += (long) currentResultSize;
        LOG.info("{} - hits:{}/{}/{} page:{} scroll-id:{} ~{}",
            lang, currentResultSize, dataCount, hitsTotal, pageNumber++,
            scrollId, delay);
      } else {
        LOG.error("Result failed: {}. Trying to proceed.",
            result.getErrorMessage());
      }
    } while (currentResultSize == ES_CONF.PAGE_SIZE);

    writer.close();
  }

  /**
   * Index search results from a query.
   * @param writer Lucene index writer
   * @param lang Language that gets indexed
   * @param hits JSON array of search hits
   * @throws IOException Thrown, if writing the Lucene index fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private void indexResults(final IndexWriter writer, final SRC_LANGUAGE
      lang, final JsonArray hits)
      throws IOException {
    if (hits.size() <= 0) {
      LOG.warn("No hits! ({})", hits.size());
    }

    // go through all hits
    for (final JsonElement hit : hits) {
      // parse JSON data to model
      final Patent p = Patent.fromJson(hit.getAsJsonObject());

      // create Lucene document from model
      final Document patDoc = new Document();
      boolean hasData = false;
      patDoc.add(new StringField("_id", p.getId(), Store.NO));

      // test, if we have claim data
      if (p.hasClaims(lang)) {
        if (this.cliParams.useTermVectors) {
          patDoc.add(new VecTextField("claims", p.getClaimsAsString(lang),
              Store.NO));
        } else {
          patDoc.add(new TextField("claims", p.getClaimsAsString(lang),
              Store.NO));
        }
        hasData = true;
      }

      // test, if we have detailed description data
      if (p.hasDetd(lang)) {
        if (this.cliParams.useTermVectors) {
          patDoc.add(new VecTextField("detd", p.getDetd(lang), Store.NO));
        } else {
          patDoc.add(new TextField("detd", p.getDetd(lang), Store.NO));
        }
        hasData = true;
      }

      // check if there's something to index
      if (hasData) {
        LOG.trace("Add doc {} [claims:{} detd:{}]",
            p.getId(), p.hasClaims(lang), p.hasDetd(lang));
        writer.addDocument(patDoc);
      } else {
        LOG.warn("No data to write for docId:{}", p.getId());
      }
    }
  }

  /**
   * Check commandline parameters and run the indexer for all configured
   * languages.
   *
   * @param args Commandline arguments.
   * @throws IOException Thrown, if target directory is not accessible
   */
  private void runMain(final String[] args)
      throws IOException {
    new CmdLineParser(this.cliParams);

    //parser.parseArgument(args);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();
    // create target, if it does not exist already
    Files.createDirectories(this.cliParams.dataDir.toPath());

    // setup REST client
    final JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(new Builder(ES_CONF.URL)
        .multiThreaded(true).build());
    this.client = factory.getObject();

    // languages to index, initially empty
    Collection<SRC_LANGUAGE> runLanguages = Collections.emptyList();

    // decide which languages to index
    if (this.cliParams.onlyLang != null) {
      // create an index for a single language only
      LOG.info("Processing language '{}' only as requested by user.",
          this.cliParams.onlyLang);
      final SRC_LANGUAGE onlyLang = SRC_LANGUAGE.getByString(this
          .cliParams.onlyLang);
      if (onlyLang != null) {
        runLanguages = Collections.singletonList(onlyLang);
      } else {
        LOG.error("Unknown language '{}'.", this.cliParams.onlyLang);
      }
    } else {
      // create an index for each known language optionally skipping single ones
      runLanguages = Arrays.asList(SRC_LANGUAGE.values());
      // optionally skip languages
      if (this.cliParams.skipLang.length > 0) {
        LOG.info("Skipping languages {} as requested by user.",
            this.cliParams.skipLang);
        for (final String skipLang : this.cliParams.skipLang) {
          final SRC_LANGUAGE skipSrcLang = SRC_LANGUAGE.getByString(skipLang);
          if (skipSrcLang != null) {
            runLanguages.remove(skipSrcLang);
          }
        }
      }
    }

    // run index for each specified language
    for (final SRC_LANGUAGE lng : runLanguages) {
      try {
        indexByLanguage(lng);
      } catch (final Exception e) {
        LOG.error("Indexing failed. lang={} {}", lng, e);
      }
    }

    // close connection
    this.client.shutdownClient();
  }

  /**
   * Try to load a list of stopwords from a local file.
   * @param lang Language to load the stopwords for
   * @return Set of stopwords
   * @throws IOException Thrown, if reading the source file fails
   */
  private Set<String> getStopwords(final String lang)
      throws IOException {
    Set<String> sWords = Collections.emptySet();
    if (this.cliParams.stopFilePattern != null) {
      // read stopwords
      final Format stopFileFormat = StopwordsFileReader.getFormatFromString(this
          .cliParams.stopFileFormat);
      if (stopFileFormat != null) {
        final File stopFile = new File(this.cliParams.stopFilePattern + "_" +
            StringUtils.lowerCase(lang) + ".txt");
        if (stopFile.exists() && stopFile.isFile()) {
          final Set<String> newWords = StopwordsFileReader.readWords
              (stopFileFormat, stopFile.getCanonicalPath(),
                  StandardCharsets.UTF_8);
          if (newWords != null) {
            LOG.info("Loaded {} stopwords. file={} lang={}", newWords.size(),
                stopFile, lang);
            sWords = newWords;
          }
        } else {
          LOG.error("Stopwords file '{}' not found. lang={}", stopFile, lang);
          System.exit(1);
        }
      }
    } else {
      LOG.info("No stopwords provided. lang={}", lang);
    }
    return sWords;
  }

  /**
   * Main method.
   * @param args Commandline arguments
   * @throws Exception Forwarded
   */
  public static void main(final String[] args)
      throws Exception {
    new BuildIndex().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Directory for storing working data.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = CliParams.DATA_DIR_P, metaVar = CliParams.DATA_DIR_M,
        required = true, usage = CliParams.DATA_DIR_U)
    File dataDir;

    /**
     * Pattern for stopwords files.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-stop", metaVar = "pattern", required = false,
        usage = "File naming pattern for stopword lists. " +
            "The pattern will be suffixed by '_<lang>.txt' (all lower case). " +
            "Stopword files are expected to be UTF-8 encoded.")
    String stopFilePattern;

    /**
     * Stopwords file format.
     */
    @SuppressWarnings({"PackageVisibleField", "FieldMayBeStatic"})
    @Option(name = "-stop-format", metaVar = "(plain|snowball)",
        required = false, depends = "-stop",
        usage = "Format of the stopwords file. 'plain' for a simple list of " +
            "each stopword per line. 'snowball' for a list of words and " +
            "comments starting with '|'. Defaults to 'plain'.")
    String stopFileFormat = "plain";

    /**
     * Single languages.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-only-lang", metaVar = "language", required = false,
        usage = "Process only the defined language. Overrides -skip-lang'.")
    String onlyLang;

    /**
     * Single languages.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-term-vectors", required = false,
        usage = "If provided term vectors will be stored for text fields.")
    boolean useTermVectors = false;

    /**
     * Skip languages.
     */
    @SuppressWarnings({"PackageVisibleField", "ZeroLengthArrayAllocation"})
    @Option(name = "-skip-lang", metaVar = "<languages>", required = false,
        handler = StringArrayOptionHandler.class,
        usage = "Skip the listed languages while processing. Separate each " +
            "language by a space.")
    String[] skipLang = {};

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     */
    void check() {
      if (!this.dataDir.exists()) {
        LOG.info("Data directory'{}' does not exist and will be created.",
            this.dataDir);
      } else if (!this.dataDir.isDirectory()) {
        LOG.error("Data directory'{}' does not look like a directory.",
            this.dataDir);
        System.exit(1);
      }

      if (StopwordsFileReader.getFormatFromString(this.stopFileFormat) ==
          null) {
        LOG.error(
            "Unknown stopwords file format '" + this.stopFileFormat + "'.");
        System.exit(-1);
      }
    }
  }
}
