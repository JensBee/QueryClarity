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
import de.unihildesheim.iw.fiz.SimpleTermFilter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
import java.util.Iterator;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class DumpSimpleTermFilter
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(DumpSimpleTermFilter.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  private DumpSimpleTermFilter() {
    super("Dump SimpleTermFilter results");
  }

  public static void main(final String... args)
      throws Exception {
    new DumpSimpleTermFilter().runMain(args);
  }

  private void runMain(final String... args)
      throws Exception {
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

    final SimpleTermFilter stf = new SimpleTermFilter();

    final DirectoryReader reader = DirectoryReader.open(
        FSDirectory.open(this.cliParams.idxDir.toPath()));
    final Fields fields = MultiFields.getFields(reader);
    final Iterator<String> fieldsIt = fields.iterator();
    TermsEnum termsEnum = null;

    while (fieldsIt.hasNext()) {
      final String fieldName = fieldsIt.next();
      if ("claims".equalsIgnoreCase(fieldName) ||
          "detd".equalsIgnoreCase(fieldName)) {
        final Terms terms = fields.terms(fieldName);
        termsEnum = terms.iterator(termsEnum);
        BytesRef term = termsEnum.next();
        while (term != null) {
          final String termStr = term.utf8ToString();
          if (stf.isAccepted(termsEnum, term)) {
            if (!this.cliParams.skipOnly) {
              System.out.println("ACCEPT: " + termStr);
            }
          } else {
            if (this.cliParams.skipOnly) {
              System.out.println(termStr);
            } else {
              System.out.println("SKIP: " + termStr);
            }
          }
          term = termsEnum.next();
        }
      }
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

    @Option(name = "-skiponly", required = false,
        usage = "Dump only skipped terms")
    boolean skipOnly = false;

    /**
     * Directory containing the target Lucene index.
     */
    @Option(name = CliParams.INDEX_DIR_P, metaVar = CliParams.INDEX_DIR_M,
        required = true, usage = CliParams.INDEX_DIR_U)
    File idxDir;

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
