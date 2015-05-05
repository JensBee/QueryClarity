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
import de.unihildesheim.iw.cli.CliCommon;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import de.unihildesheim.iw.fiz.models.Patent;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder;
import de.unihildesheim.iw.util.StopwordsFileReader;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig.Builder;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.jetbrains.annotations.NotNull;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

/**
 * Builds a language specific Lucene index from all documents queried from a
 * remote documents repository.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
final class BuildIndex
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(BuildIndex.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * Client to ElasticSearch instance.
   */
  private JestClient client;
  /**
   * Used to generate randomized delays for request throttling.
   */
  private final Random rand = new Random();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private BuildIndex() {
    super("Create local term index.",
        "Create the local term index from the remote patents repository.");
  }

  /**
   * Index the documents for a specific language.
   *
   * @param lang Language to index.
   * @throws Exception Thrown on REST request errors
   */
  @SuppressWarnings({"ObjectAllocationInLoop", "BusyWait"})
  private void indexByLanguage(@NotNull final Language lang)
      throws Exception {
    LOG.info("Creating index for: {}", lang);

    // claim by language
    final String fld_claim = ES_CONF.FLD_CLAIM_PREFIX + lang;

    // get all claims from patents in the specific language including
    // detailed technical description, if available
    @SuppressWarnings("HardcodedLineSeparator")
    final String query =
        "{\n" +
            // match all documents - filter later on
            "  \"query\": {\n" +
            "    \"match_all\": {}\n" +
            "  },\n" +

            // fields to return
            "  \"fields\": [\n" +
            // f: claims
            "    \"" + fld_claim + "\",\n" +
            // f: detailed description
            "    \"" + ES_CONF.FLD_DESC + "\",\n" +
            // f: detailed description language
            "    \"" + ES_CONF.FLD_DESC_LNG + "\",\n" +
            // f: patent id
            "    \"" + ES_CONF.FLD_PATREF + "\",\n" +
            // f: ipc code(s)
            "    \"" + ES_CONF.FLD_IPC + "\"\n" +
            "  ],\n" +

            "  \"filter\": {\n" +
            // document requires to have claims or detd in target language
            "    \"or\": [\n" +
            //     condition: claims in target language
            "      {\n" +
            "        \"exists\": {\n" +
            "          \"field\": \"" + fld_claim + "\"\n" +
            "        }\n" +
            "      },\n" +
            //     condition: detd in target language
            "      {\n" +
            "        \"and\": [\n" +
            //         match language
            "          {\n" +
            "            \"term\": {\n" +
            "              \"" + ES_CONF.FLD_DESC_LNG + "\": " +
            "                 \"" + lang + "\"\n" +
            "            }\n" +
            "          },\n" +
            //         require field to exist
            "          {\n" +
            "            \"exists\": {\n" +
            "              \"field\": \"" + ES_CONF.FLD_DESC + "\"\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            '}';

    // setup the search using scan & scroll
    final Search search = new Search.Builder(query)
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
    JestResult result = ESUtils.runRequest(this.client, search);

    if (!result.isSucceeded()) {
      LOG.error("Initial request failed. {}", result.getErrorMessage());
    }

    JsonObject resultJson = result.getJsonObject();
    JsonArray hits = resultJson.getAsJsonObject("hits").getAsJsonArray("hits");

    String scrollId = resultJson.get("_scroll_id").getAsString();
    final String hitsTotal = resultJson.getAsJsonObject("hits")
        .get("total").toString();
    int currentResultSize = hits.size();

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} - hits:{}/{} scroll-id:{}", lang, currentResultSize,
          hitsTotal, scrollId);
    }

    final Path targetPath = new File(this.cliParams.dataDir.getAbsolutePath() +
        File.separator + lang).toPath();
    Files.createDirectories(targetPath);
    final IndexBuilder iBuilder = new IndexBuilder(targetPath, lang,
        CliCommon.getStopwords(lang.toString(),
            this.cliParams.stopFileFormat, this.cliParams.stopFilePattern));
    indexResults(iBuilder, hits);

    // scroll through pages to gather all results
    int pageNumber = 1; // number of pages requested
    long dataCount = (long) currentResultSize; // number of items retrieved
    int delay; // throttle delay
    do {
      // retry a request if it has timed out
      delay = (1 + this.rand.nextInt(10)) * 100;

      Thread.sleep((long) delay);
      result = ESUtils.runRequest(this.client, new SearchScroll.Builder
          (scrollId, ES_CONF.SCROLL_KEEP).build());

      if (result.isSucceeded()) {
        // parse result set
        resultJson = result.getJsonObject();
        hits = resultJson.getAsJsonObject("hits").getAsJsonArray("hits");
        currentResultSize = hits.size();
        scrollId = resultJson.get("_scroll_id").getAsString();

        // index results
        indexResults(iBuilder, hits);
        dataCount += (long) currentResultSize;

        LOG.info("{} - hits:{}/{}/{} page:{} scroll-id:{} ~{}",
            lang, currentResultSize, dataCount, hitsTotal, pageNumber++,
            scrollId, delay);
      } else {
        LOG.error("Result failed: {}. Trying to proceed.",
            result.getErrorMessage());
      }
    } while (currentResultSize == ES_CONF.PAGE_SIZE);

    iBuilder.close();
  }

  /**
   * Index search results from a query.
   *
   * @param writer Lucene index writer
   * @param hits JSON array of search hits
   * @throws IOException Thrown, if writing the Lucene index fails
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  private static void indexResults(
      @NotNull final IndexBuilder writer,
      @NotNull final JsonArray hits)
      throws IOException {
    if (hits.size() <= 0) {
      LOG.warn("No hits! ({})", hits.size());
      return;
    }

    // go through all hits
    for (final JsonElement hit : hits) {
      // parse JSON data to model
      writer.index(Patent.fromJson(hit.getAsJsonObject()));
    }
  }

  /**
   * Check commandline parameters and run the indexer for all configured
   * languages.
   *
   * @param args Commandline arguments.
   * @throws IOException Thrown, if target directory is not accessible
   */
  private void runMain(@NotNull final String... args)
      throws Exception {
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
    Collection<Language> runLanguages = Collections.emptyList();

    // decide which languages to index
    if (this.cliParams.onlyLang != null) {
      // create an index for a single language only
      LOG.info("Processing language '{}' only as requested by user.",
          this.cliParams.onlyLang);
      final Language onlyLang = Language.getByString(this
          .cliParams.onlyLang);
      if (onlyLang != null) {
        runLanguages = Collections.singletonList(onlyLang);
      } else {
        LOG.error("Unknown language '{}'.", this.cliParams.onlyLang);
      }
    } else {
      // create an index for each known language optionally skipping single ones
      runLanguages = Arrays.asList(Language.values());
      // optionally skip languages
      if (this.cliParams.skipLang.length > 0) {
        LOG.info("Skipping languages {} as requested by user.",
            this.cliParams.skipLang);
        for (final String skipLang : this.cliParams.skipLang) {
          final Language skipSrcLang = Language.getByString(skipLang);
          if (skipSrcLang != null) {
            runLanguages.remove(skipSrcLang);
          }
        }
      }
    }

    // run index for each specified language
    for (final Language lng : runLanguages) {
      try {
        indexByLanguage(lng);
      } catch (final Exception e) {
        LOG.error("Indexing failed. lang={}", lng, e);
        throw e;
      }
    }

    // close connection
    this.client.shutdownClient();
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments
   * @throws Exception Forwarded
   */
  public static void main(@NotNull final String... args)
      throws Exception {
    new BuildIndex().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        org.slf4j.LoggerFactory.getLogger(Params.class);
    /**
     * Directory for storing working data.
     */
    @Option(name = CliParams.DATA_DIR_P, metaVar = CliParams.DATA_DIR_M,
        required = true, usage = CliParams.DATA_DIR_U)
    File dataDir;

    /**
     * Pattern for stopwords files.
     */
    @Option(name = "-stop", metaVar = "pattern", required = false,
        usage = "File naming pattern for stopword lists. " +
            "The pattern will be suffixed by '_<lang>.txt' (all lower case). " +
            "Stopword files are expected to be UTF-8 encoded.")
    String stopFilePattern = "";

    /**
     * Stopwords file format.
     */
    @Option(name = "-stop-format", metaVar = "(plain|snowball)",
        required = false, depends = "-stop",
        usage = "Format of the stopwords file. 'plain' for a simple list of " +
            "each stopword per line. 'snowball' for a list of words and " +
            "comments starting with '|'. Defaults to 'plain'.")
    String stopFileFormat = "plain";

    /**
     * Single languages.
     */
    @Option(name = "-only-lang", metaVar = "language", required = false,
        usage = "Process only the defined language. Overrides -skip-lang'.")
    String onlyLang;

    /**
     * Skip languages.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Option(name = "-skip-lang", metaVar = "<languages>", required = false,
        handler = StringArrayOptionHandler.class,
        usage = "Skip the listed languages while processing. Separate each " +
            "language by a space.")
    String[] skipLang = {};

    /**
     * Empty constructor to allow access from outer class.
     */
    Params() {
      // empty
    }

    /**
     * Check, if the defined files and directories are available.
     */
    void check() {
      assert this.dataDir != null;
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
        LOG.error("Unknown stopwords file format '" +
            this.stopFileFormat + "'.");
        System.exit(-1);
      }
    }
  }
}
