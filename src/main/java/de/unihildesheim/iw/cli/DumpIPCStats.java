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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.util.TaskObserver;
import de.unihildesheim.iw.util.TaskObserver.TaskObserverMessage;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Commandline utility to dump statistics for all IPC-codes from the Lucene
 * index.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class DumpIPCStats
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DumpIPCStats.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private DumpIPCStats() {
    super("Dump statistics for all IPC codes from the index.",
        "Dump detailed statistics for all IPC codes from the index.");
  }

  public static void main(final String... args)
      throws IOException, Buildable.BuildException {
    new DumpIPCStats().runMain(args);
  }

  @SuppressWarnings("HardcodedLineSeparator")
  private void runMain(final String... args)
      throws IOException, Buildable.BuildException {
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

    final Bits liveDocs = MultiFields.getLiveDocs(reader);
    final Set<String> fields =
        Collections.singleton(LUCENE_CONF.FLD_IPC);

    // index of all IPC-codes found
    final Map<IPCRecord, Integer> ipcCodeStats =
        new ConcurrentHashMap<>(70000);
    // IPC-codes count by section
    final Map<String, Integer> ipcCodeStatsBySection =
        new ConcurrentHashMap<>(10);

    final Map<IPCRecord, Integer> ipcCodeStatsDetailed =
        new ConcurrentHashMap<>(100000);

    @SuppressWarnings("AnonymousInnerClassMayBeStatic")
    final TaskObserver obs = new TaskObserver(
        new TaskObserverMessage() {
          @Override
          public void call(@NotNull final TimeMeasure tm) {
            LOG.info("Collected {} IPC-codes after {}.",
                NumberFormat.getIntegerInstance().format(
                    (long) ipcCodeStats.size()),
                tm.getTimeString());
          }
        }).start();

    IntStream.range(0, reader.maxDoc())
        .parallel()
            // check, if document with current id exists
        .filter(id -> liveDocs == null || liveDocs.get(id))
            // get the document field values we're interested in
        .mapToObj(id -> {
          try {
            return reader.document(id, fields);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .forEach(doc -> {
          final List<IPCRecord> records =
              Arrays.stream(doc.getValues(LUCENE_CONF.FLD_IPC))
                  .map(ipcParser::parse).collect(Collectors.toList());

          // increase counter for each code we've found
          records.stream().forEach(code ->
              ipcCodeStats.compute(code, (k, v) -> v == null ? 1 : v + 1));
        });

    obs.stop();
    LOG.info("Parsing results");

    // count entries by IPC-section
    ipcCodeStats.entrySet().parallelStream()
        .forEach(e -> {
          final String section = e.getKey().get(Field.SECTION);
          ipcCodeStatsBySection.compute(section,
              (k, v) -> v == null ? e.getValue() : v + e.getValue());
        });

    ipcCodeStats.entrySet().parallelStream()
        .forEach(e -> {
          final Collection<IPCRecord> recs = new HashSet<>(5);
          final StringBuilder code = new StringBuilder(IPCRecord.MAX_LENGTH);

          // section
          final String sec = e.getKey().get(Field.SECTION);
          if (!sec.isEmpty()) {
            code.append(sec);
            recs.add(IPCCode.parse(code));

            // class
            final String cls = e.getKey().get(Field.CLASS);
            if (!cls.isEmpty()) {
              code.append(cls);
              recs.add(IPCCode.parse(code));

              // subclass
              final String sCls = e.getKey().get(Field.SUBCLASS);
              if (!sCls.isEmpty()) {
                code.append(sCls);
                recs.add(IPCCode.parse(code));

                // main-group
                final String mGrp = e.getKey().get(Field.MAINGROUP);
                if (!mGrp.isEmpty()) {
                  code.append(mGrp);
                  recs.add(IPCCode.parse(code));

                  // sub-group
                  final String sGrp = e.getKey().get(Field.SUBGROUP);
                  if (!sGrp.isEmpty()) {
                    code.append(Parser.DEFAULT_SEPARATOR).append(sGrp);
                    recs.add(IPCCode.parse(code));
                  }
                }
              }
            }
          }

          recs.forEach(rec -> {
            ipcCodeStatsDetailed.compute(rec,
                (k, v) -> v == null ? e.getValue() : v + e.getValue());
          });
        });

    System.out.println("== All IPCs ==");
    ipcCodeStats.entrySet().stream()
        .sorted((o1, o2) ->
            IPCRecord.COMPARATOR.compare(o1.getKey(), o2.getKey()))
        .forEach(e -> System.out.println(
            "ipc=" + e.getKey().toFormattedString() +
                " count=" + e.getValue()));

    System.out.println("\n== IPCs by Section ==");
    ipcCodeStatsBySection.entrySet().stream()
        .forEach(e -> System.out.println(
            "section=" + e.getKey() + " count=" + e.getValue()));

    System.out.println("\n== Detailed distribution ==");
    ipcCodeStatsDetailed.entrySet().stream()
        .sorted((o1, o2) ->
            IPCRecord.COMPARATOR.compare(o1.getKey(), o2.getKey()))
        .forEach(e -> System.out.println(
            "ipc=" + e.getKey() + " count=" + e.getValue()));
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

    Params() {
    }

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
