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

package de.unihildesheim.iw.cli.tools;

import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.cli.CliParams;
import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.query.IPCClassQuery;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreResult;
import de.unihildesheim.iw.lucene.scoring.data.CommonTermsFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter;
import de.unihildesheim.iw.lucene.search.IPCFieldFilterFunctions;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.GlobalConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class CheckFeedback extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(CheckFeedback.class);
  /**
   * Object wrapping commandline options.
   */
  final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private CheckFeedback() {
    super("Analyze Feedback for a query");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String... args)
      throws Exception {
    new CheckFeedback().runMain(args);
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

    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}",
        this.cliParams.language, this.cliParams.docFields);

    // init filtered reader
    final FilteredDirectoryReader.Builder
        fIdxReaderBuilder = new FilteredDirectoryReader
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
      fIdxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
    }

    // finally build the reader
    final FilteredDirectoryReader fIdxReader = fIdxReaderBuilder.build();

    // Should we include IPC-data?
    final boolean includeIPC = this.cliParams.ipcRec != null;

    try (final FDRIndexDataProvider dataProv = new FDRIndexDataProvider
        .Builder()
        .indexReader(fIdxReader)
        .build()) {
      // lucene analyzer
      final Analyzer analyzer = LanguageBasedAnalyzers.createInstance(
          this.cliParams.language, CharArraySet.EMPTY_SET);

      LOG.info(
          "Using CommonTermsFeedbackProvider & DefaultVocabularyProvider.");
      final FeedbackProvider fbProv = new CommonTermsFeedbackProvider();
      fbProv
          .dataProvider(dataProv)
          .indexReader(fIdxReader)
          .analyzer(analyzer);
      final VocabularyProvider vocProv = new DefaultVocabularyProvider();
      vocProv.indexDataProvider(dataProv);

      // get a normalized unique list of query terms
      // skips stopwords and removes unknown terms (not visible in current
      // fields, etc.)
      final BytesRefArray queryTerms = QueryUtils.tokenizeQuery(
          this.cliParams.query, analyzer, dataProv);
      // check query term extraction result
      if (queryTerms.size() == 0) {
        LOG.warn(ClarityScoreResult.EmptyReason.NO_QUERY_TERMS.toString());
        return;
      }

      LOG.info("Query: {}", this.cliParams.query);

      DocIdSet feedbackDocIds;
      int fbDocCount;
      try {
        feedbackDocIds = fbProv
            .query(queryTerms)
            .fields(dataProv.getDocumentFields())
            .amount(1, this.cliParams.p_fbMax)
            .get();
        fbDocCount = DocIdSetUtils.cardinality(feedbackDocIds);
        LOG.info("Feedback documents: {}", fbDocCount);
      } catch (final BooleanQuery.TooManyClauses e) {
        LOG.warn(ClarityScoreResult.EmptyReason.TOO_MANY_BOOLCLAUSES.toString());
        return;
      } catch (final Exception e) {
        final String msg = "Caught exception while getting feedback documents.";
        LOG.error(msg, e);
        throw new ClarityScoreCalculation.ClarityScoreCalculationException(msg, e);
      }

      LOG.info("start:Terms for each document");
      StreamUtils.stream(feedbackDocIds).forEach(d -> {
        final Stream<BytesRef> docTerms = dataProv.getDocumentTerms(
            d, this.cliParams.fields);
        LOG.info("Doc id:{} - vis in index? {}", d, dataProv.hasDocument(d));
        final StringBuilder sb = new StringBuilder();
        docTerms.forEach(t -> sb.append(t.utf8ToString()).append(' '));
        LOG.info("Doc id:{} - terms(i):{}", d, sb);

        try {
          final StringBuilder sb3 = new StringBuilder();

          final Fields tv = fIdxReader.getTermVectors(d);
          final Iterator<String> tvIt = tv.iterator();

          while (tvIt.hasNext()) {
            final String f = tvIt.next();
            sb3.append(f);
            final Terms t = fIdxReader.getTermVector(d, f);
            if (t != null) {
              final TermsEnum te = t.iterator(null);
              BytesRef term;
              int cnt = 0;
              while ((term = te.next()) != null) {
                cnt++;
              }
              sb3.append('(').append(cnt).append(')');
            } else {
              sb3.append("(empty) ");
            }
          }
//
//          final List<IndexableField> flds = fIdxReader.document(d)
//              .getFields();
//          for (final IndexableField f : flds) {
//            sb3.append(f.name()).append(' ');
//          }
          LOG.info("Fields (tv) d:{} f:{}",d,sb3);
        } catch (final Exception e) {
          LOG.error("Error getting fields! d:{}",d);
        }

//        final DocumentModel dm = dataProv.getDocumentModel(d);
//        final StringBuilder sb2 = new StringBuilder();
//        StreamUtils.stream(dm.getTermsForSerialization()).forEach(t -> sb2
//            .append(t.utf8ToString()).append(' '));
//        LOG.info("Doc id:{} - terms(d):{}", d, sb2);
      });
      LOG.info("stop:Terms for each document");

      final Stream<BytesRef> fbTermStream = vocProv
          .documentIds(feedbackDocIds).get();

      LOG.info("start:Feedback Terms");
      fbTermStream.forEach(t -> {
        final double relDf = dataProv.getRelativeDocumentFrequency(t);
        LOG.info("Term {} - {}", t.utf8ToString(), relDf);
      });
      LOG.info("stop:Feedback Terms");
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
     * Query.
     */
    @Option(name = "-q", metaVar = "<query>", required = true)
    String query;

    /**
     * Language to process.
     */
    @Option(name = "-lang", metaVar = "<language>", required = true)
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

    @Option(name = "-fbmax", required = true,
        usage = "Maximum number of feedback documents (int).")
    Integer p_fbMax;

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

    /**
     * Check commandline parameters.
     *
     * @throws IOException Thrown on low-level i/o-errors or a Lucene index was
     * not found at the specified location
     */
    void check()
        throws IOException {
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

      this.docFields = new HashSet<>(this.fields.length);
      Collections.addAll(this.docFields, this.fields);
    }
  }
}
