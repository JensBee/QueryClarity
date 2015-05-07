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

import de.unihildesheim.iw.Buildable.BuildException;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader.Builder;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.lucene.query.IPCClassQuery;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter;
import de.unihildesheim.iw.lucene.search.IPCFieldFilterFunctions;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static de.unihildesheim.iw.data.IPCCode.IPCRecord;

/**
 * Commandline utility to dump all IPC-codes from the Lucene index.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class DumpIPCs
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(DumpIPCs.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private DumpIPCs() {
    super("Dump all IPC codes from the index.",
        "Dump all IPC codes from the index.");
  }

  public static void main(final String... args)
      throws IOException, BuildException {
    new DumpIPCs().runMain(args);
  }

  private void runMain(final String... args)
      throws IOException, BuildException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    assert this.cliParams.idxReader != null;
    final int maxDoc = this.cliParams.idxReader.maxDoc();
    if (maxDoc == 0) {
      LOG.error("Empty index.");
      return;
    }

    final Parser ipcParser = new Parser();
    ipcParser.separatorChar(this.cliParams.sep);
    ipcParser.allowZeroPad(this.cliParams.zeroPad);

    final DirectoryReader reader = DirectoryReader.open(
        FSDirectory.open(this.cliParams.idxDir.toPath()));
    final Builder idxReaderBuilder = new Builder(reader);

    Pattern rx_ipc = null;

    if (this.cliParams.ipc != null) {
      final IPCRecord ipc = ipcParser.parse(this.cliParams.ipc);
      final BooleanQuery bq = new BooleanQuery();
      rx_ipc = Pattern.compile(ipc.toRegExpString(this.cliParams.sep));
      if (LOG.isDebugEnabled()) {
        LOG.debug("IPC regExp: rx={} pat={}",
            ipc.toRegExpString(this.cliParams.sep),
            rx_ipc);
      }

      bq.add(new QueryWrapperFilter(
          IPCClassQuery.get(ipc, this.cliParams.sep)), Occur.MUST);
      bq.add(new QueryWrapperFilter(
          new IPCFieldFilter(
              new IPCFieldFilterFunctions.SloppyMatch(ipc), ipcParser
          )), Occur.MUST);
      idxReaderBuilder.queryFilter(new QueryWrapperFilter(bq));
    }

    final IndexReader idxReader = idxReaderBuilder.build();

    if (idxReader.numDocs() > 0) {
      final Terms terms =
          MultiFields.getTerms(idxReader, LUCENE_CONF.FLD_IPC);
      TermsEnum termsEnum = TermsEnum.EMPTY;
      BytesRef term;
      if (terms != null) {
        termsEnum = terms.iterator(termsEnum);
        term = termsEnum.next();

        final int[] count = {0, 0}; // match, exclude
        while (term != null) {
          final String code = term.utf8ToString();
          if (rx_ipc == null || (rx_ipc.matcher(code).matches())) {
            final IPCRecord record = ipcParser.parse(code);
            try {
              System.out.println(
                  code + ' ' + record +
                      " (" + record.toFormattedString() + ") " +
                      '[' + record.toRegExpString('-') + ']');
            } catch (final IllegalArgumentException e) {
              System.out.println(code + ' ' + "INVALID (" + code + ')');
            }
            count[0]++;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skip non matching IPC: {}", code);
            }
            count[1]++;
          }
          term = termsEnum.next();
        }
        LOG.info("match={} skip={}", count[0], count[1]);
      }
    } else {
      LOG.info("No documents left after filtering.");
    }
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Params.class);

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

    /**
     * IPC code.
     */
    @Nullable
    @Option(name = "-ipc", metaVar = "IPC", required = false,
        usage = "IPC-code (fragment) to filter returned codes.")
    String ipc;

    /**
     * Default separator char.
     */
    @Option(name = "-grpsep", metaVar = "[separator char]",
        required = false,
        usage = "Char to use for separating main- and sub-group.")
    char sep = Parser.DEFAULT_SEPARATOR;

    /**
     * Allow zero padding.
     */
    @Option(name = "-zeropad", required = false,
        usage = "Allows padding of missing information with zeros.")
    boolean zeroPad = false;

    /**
     * {@link Directory} instance pointing at the Lucene index.
     */
    @Nullable
    private Directory luceneDir;

    /**
     * {@link IndexReader} to use for accessing the Lucene index.
     */
    @Nullable
    IndexReader idxReader;

    /**
     * Check, if the defined files and directories are available.
     *
     * @throws IOException Thrown on low-level i/o-errors
     */
    void check()
        throws IOException {
      assert this.idxDir != null;
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
        LOG.error("Index directory '{}' does not exist.", this.idxDir);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
