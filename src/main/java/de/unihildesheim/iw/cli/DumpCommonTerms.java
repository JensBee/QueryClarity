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

package de.unihildesheim.iw.cli;

import au.com.bytecode.opencsv.CSVWriter;
import de.unihildesheim.iw.Buildable.BuildException;
import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.util.StopwordsFileReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

/**
 * Dump terms that will be skipped when using a CommonTerms feedback provider
 * oder query class. This will not exactly list those terms, but still gives an
 * impression on what may get excluded.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class DumpCommonTerms
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DumpCommonTerms.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();
  /**
   * CSV writer instance.
   */
  private CSVWriter csvWriter;

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private DumpCommonTerms() {
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
      throws BuildException, ConfigurationException, IOException,
             DataProviderException {
    new DumpCommonTerms().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws IOException
   */
  private void runMain(final String[] args)
      throws IOException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    LOG.info("Writing terms to '{}'.", this.cliParams.targetFile);

    final Set<String> sWords = CliCommon.getStopwords(this.cliParams.lang,
        this.cliParams.stopFileFormat, this.cliParams.stopFilePattern);

    final int maxDoc = this.cliParams.idxReader.maxDoc();
    if (maxDoc == 0) {
      LOG.error("Empty index.");
      return;
    }

    final Terms terms = MultiFields.getTerms(this.cliParams.idxReader,
        this.cliParams.field);
    TermsEnum termsEnum = TermsEnum.EMPTY;
    BytesRef term;

    if (terms != null) {
      termsEnum = terms.iterator(termsEnum);
      term = termsEnum.next();

      try {
        // target file
        this.csvWriter =
            new CSVWriter(new FileWriter(this.cliParams.targetFile));
        // write header line
        this.csvWriter.writeNext(new String[]{"term", "relDF"});

        while (term != null) {
          final String termStr = term.utf8ToString();
          if (!sWords.contains(termStr.toLowerCase())) {
            final double docFreq = (double) termsEnum.docFreq();
            if (docFreq > 0d) {
              final double relDocFreq = docFreq / (double) maxDoc;

              if (relDocFreq > this.cliParams.threshold) {
                // log term
                this.csvWriter.writeNext(new String[]{
                    termStr,
                    // make exponential string R compatible
                    Double.toString(relDocFreq).toLowerCase()
                });
              }
            }
          }
          term = termsEnum.next();
        }
      } finally {
        this.csvWriter.close();
      }
    }

    /*
    final Set<String> sWords = CliCommon.getStopwords(this.cliParams.lang,
        this.cliParams.stopFileFormat, this.cliParams.stopFilePattern);

    // suffix all fields by language
    final Set<String> langFields = Collections.singleton(this.cliParams.field);

    // create the IndexDataProvider
    LOG.info("Initializing IndexDataProvider. lang={} fields={}",
        this.cliParams.lang, langFields);
    final DirectAccessIndexDataProvider dataProv = new Builder()
        .documentFields(langFields)
        .indexPath(this.cliParams.idxDir.getCanonicalPath())
        .stopwords(sWords)
        .build();
    final CollectionMetrics cMetrics = new Metrics(dataProv).collection();

    try {
      // target file
      this.csvWriter = new CSVWriter(new FileWriter(this.cliParams.targetFile));
      // write header line
      this.csvWriter.writeNext(new String[]{"term", "relDF"});

      // iterate through terms
      final Iterator<ByteArray> termsIt = dataProv.getTermsIterator();
      final String termStr;
      while (termsIt.hasNext()) {
        final ByteArray termBa = termsIt.next();
        final float relDf = cMetrics.relDf(termBa).floatValue();
        if (relDf > this.cliParams.threshold) {
          // log term
          this.csvWriter.writeNext(new String[]{
              ByteArrayUtils.utf8ToString(termBa),
              // make exponential string R compatible
              Float.toString(relDf).toLowerCase()
          });
        }
      }
    } finally {
      this.csvWriter.close();
    }
    */
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Stopwords file format.
     */
    @SuppressWarnings({"PackageVisibleField", "FieldMayBeStatic"})
    @Option(name = "-stop-format", metaVar = "(plain|snowball)", required =
        false, depends = "-stop", usage =
        "Format of the stopwords file. 'plain' for a simple list of " +
            "each stopword per line. 'snowball' for a list of words and " +
            "comments starting with '|'. Defaults to 'plain'.")
    String stopFileFormat = "plain";

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
     * {@link Directory} instance pointing at the Lucene index.
     */
    private Directory luceneDir;
    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    @SuppressWarnings("PackageVisibleField")
    IndexReader idxReader;

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
    @Option(name = "-field", metaVar = "field name", required = true,
        handler = StringArrayOptionHandler.class,
        usage = "Document field to query.")
    String field;

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
     * Single language.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process for the defined language.")
    String lang;

    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-threshold", metaVar = "float", required = true,
        usage = "Document frequency threshold. If this is exceeded a term " +
            "will be treated as being too common (means gets skipped).")
    double threshold;

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     * @throws IOException
     */
    void check()
        throws IOException {
      if (this.targetFile.exists()) {
        LOG.error("Target file '" + this.targetFile + "' already exist.");
        System.exit(-1);
      }
      if (this.idxDir.exists()) {
        // check, if path is a directory
        if (!this.idxDir.isDirectory()) {
          throw new IOException("Index path '" + this.idxDir
              + "' exists, but is not a directory.");
        }
        // check, if there's a Lucene index in the path
        this.luceneDir = FSDirectory.open(this.idxDir);
        if (!DirectoryReader.indexExists(this.luceneDir)) {
          throw new IOException("No index found at index path '" +
              this.idxDir.getCanonicalPath() + "'.");
        }
        this.idxReader = DirectoryReader.open(this.luceneDir);
      } else {
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
    }
  }

}
