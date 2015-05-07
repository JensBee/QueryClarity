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

import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.storage.sql.IPCStats.AllIPCTable;
import de.unihildesheim.iw.storage.sql.IPCStats.IPCDistributionTable;
import de.unihildesheim.iw.storage.sql.IPCStats.IPCPerDocumentTable;
import de.unihildesheim.iw.storage.sql.IPCStats.IPCSectionsTable;
import de.unihildesheim.iw.storage.sql.IPCStats.IPCStatsDB;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.Table;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.util.StringUtils;
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
import java.sql.SQLException;
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
      throws IOException, SQLException, ClassNotFoundException {
    new DumpIPCStats().runMain(args);
  }

  @SuppressWarnings({"HardcodedLineSeparator",
      "UnnecessarilyQualifiedInnerClassAccess"})
  private void runMain(final String... args)
      throws IOException, SQLException, ClassNotFoundException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    LOG.info("Using index at {}", this.cliParams.idxDir.getAbsolutePath());
    if (this.cliParams.targetDb != null) {
      LOG.info("Writing results to {}",
          this.cliParams.targetDb.getAbsoluteFile());
    }

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
    // detailed IPC-code statistics
    final Map<IPCRecord, Integer> ipcCodeStatsDetailed =
        new ConcurrentHashMap<>(100000);

    // record number of IPC codes assigned per document
    final Map<Integer, Integer> numberOfIpcsPerDoc =
        new ConcurrentHashMap<>(100);

    // record number of IPC section codes assigned per document
    final Map<Integer, Integer> numberOfIpcSectionsPerDoc =
        new ConcurrentHashMap<>(IPCRecord.MAX_LENGTH << 1);

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

          // store/in counter for number of IPC-codes assigned
          numberOfIpcsPerDoc.compute(
              records.size(), (k, v) -> v == null ? 1 : v + 1);

          // store/in counter for number of unique IPC-sections assigned
          final Collection<String> secs = records.stream()
              .map(code -> code.get(Field.SECTION))
              .collect(Collectors.toSet());
          numberOfIpcSectionsPerDoc.compute(
              secs.size(), (k, v) -> v == null ? 1 : v + 1);

          // increase counter for each code we've found
          records.stream().forEach(code ->
              ipcCodeStats.compute(code, (k, v) -> v == null ? 1 : v + 1));
        });

    obs.stop();
    LOG.info("Parsing results");

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

    if (this.cliParams.targetDb == null) {
      // dump to console
      System.out.println("== All IPCs from index ==");
      ipcCodeStats.entrySet().stream()
          .sorted((o1, o2) ->
              IPCRecord.COMPARATOR.compare(o1.getKey(), o2.getKey()))
          .forEach(e -> System.out.println(
              "ipc=" + e.getKey().toFormattedString() +
                  " count=" + e.getValue()));

      System.out.println("\n== Detailed distribution ==");
      ipcCodeStatsDetailed.entrySet().stream()
          .sorted((o1, o2) ->
              IPCRecord.COMPARATOR.compare(o1.getKey(), o2.getKey()))
          .forEach(e -> System.out.println(
              "ipc=" + e.getKey() + " count=" + e.getValue()));

      System.out.println("\n== IPC-Codes per document ==");
      numberOfIpcsPerDoc.entrySet().stream()
          .sorted((o1, o2) ->
              Integer.compare(o1.getKey(), o2.getKey()))
          .forEach(e -> System.out.println(
              "ipc-codes=" + e.getKey() + " count=" + e.getValue()));

      System.out.println("\n== Distinct IPC-Sections per document ==");
      numberOfIpcSectionsPerDoc.entrySet().stream()
          .sorted((o1, o2) ->
              Integer.compare(o1.getKey(), o2.getKey()))
          .forEach(e -> System.out.println(
              "ipc-sections=" + e.getKey() + " count=" + e.getValue()));
    } else {
      // write SQLite
      // table manager instance: Target database for term data
      try (final IPCStatsDB db = new IPCStatsDB(this.cliParams.targetDb)) {
        final Table metaTable = new MetaTable();
        final Table ipcAll = new AllIPCTable();
        final Table ipcSecs = new IPCSectionsTable();
        final Table ipcDist = new IPCDistributionTable();
        final Table ipcPerDoc = new IPCPerDocumentTable();

        db.createTables(metaTable, ipcAll, ipcSecs, ipcDist, ipcPerDoc);

        // write meta-data
        try (final MetaTable.Writer metaWriter =
                 new MetaTable.Writer(db.getConnection())) {
          metaWriter.addContent(new TableFieldContent(metaTable)
              .setValue(MetaTable.Fields.TABLE_NAME, "ipc-stats")
              .setValue(MetaTable.Fields.CMD,
                  StringUtils.join(args, " ")));
        }

        // all IPC table
        try (final AllIPCTable.Writer dataWriter =
                 new AllIPCTable.Writer(db.getConnection())) {
          ipcCodeStats.entrySet().stream()
              .forEach(e -> {
                final IPCRecord rec = e.getKey();
                final TableFieldContent tfc = new TableFieldContent(ipcAll);
                tfc.setValue(AllIPCTable.Fields.CODE,
                    rec.toFormattedString());
                tfc.setValue(AllIPCTable.Fields.COUNT, e.getValue());

                if (!rec.get(IPCRecord.Field.SECTION).isEmpty()) {
                  tfc.setValue(AllIPCTable.Fields.SECTION,
                      rec.get(IPCRecord.Field.SECTION));

                  if (!rec.get(IPCRecord.Field.CLASS).isEmpty()) {
                    tfc.setValue(AllIPCTable.Fields.CLASS,
                        rec.get(IPCRecord.Field.CLASS));

                    if (!rec.get(IPCRecord.Field.SUBCLASS).isEmpty()) {
                      tfc.setValue(AllIPCTable.Fields.SUBCLASS,
                          rec.get(IPCRecord.Field.SUBCLASS));

                      if (!rec.get(IPCRecord.Field.MAINGROUP).isEmpty()) {
                        tfc.setValue(AllIPCTable.Fields.MAINGROUP,
                            rec.get(IPCRecord.Field.MAINGROUP));

                        if (!rec.get(IPCRecord.Field.SUBGROUP).isEmpty()) {
                          tfc.setValue(AllIPCTable.Fields.SUBGROUP,
                              rec.get(IPCRecord.Field.SUBGROUP));
                        }
                      }
                    }
                  }
                }

                try {
                  dataWriter.addContent(tfc);
                } catch (final SQLException ex) {
                  throw new RuntimeException(ex);
                }
              });
        }

        // distinct IPC sections table
        try (final IPCSectionsTable.Writer dataWriter =
                 new IPCSectionsTable.Writer(db.getConnection())) {
          numberOfIpcSectionsPerDoc.entrySet().stream()
              .forEach(e -> {
                final TableFieldContent tfc = new TableFieldContent(ipcSecs);
                tfc.setValue(IPCSectionsTable.Fields.SECTIONS, e.getKey());
                tfc.setValue(IPCSectionsTable.Fields.COUNT, e.getValue());

                try {
                  dataWriter.addContent(tfc);
                } catch (final SQLException ex) {
                  throw new RuntimeException(ex);
                }
              });
        }

        // IPC codes distribution table
        try (final IPCDistributionTable.Writer dataWriter =
                 new IPCDistributionTable.Writer(db.getConnection())) {
          ipcCodeStatsDetailed.entrySet().stream()
              .forEach(e -> {
                final TableFieldContent tfc = new TableFieldContent(ipcDist);
                final IPCRecord rec = e.getKey();
                tfc.setValue(IPCDistributionTable.Fields.CODE,
                    e.getKey().toFormattedString());
                tfc.setValue(AllIPCTable.Fields.COUNT, e.getValue());

                if (!rec.get(IPCRecord.Field.SECTION).isEmpty()) {
                  tfc.setValue(AllIPCTable.Fields.SECTION,
                      rec.get(IPCRecord.Field.SECTION));

                  if (!rec.get(IPCRecord.Field.CLASS).isEmpty()) {
                    tfc.setValue(AllIPCTable.Fields.CLASS,
                        rec.get(IPCRecord.Field.CLASS));

                    if (!rec.get(IPCRecord.Field.SUBCLASS).isEmpty()) {
                      tfc.setValue(AllIPCTable.Fields.SUBCLASS,
                          rec.get(IPCRecord.Field.SUBCLASS));

                      if (!rec.get(IPCRecord.Field.MAINGROUP).isEmpty()) {
                        tfc.setValue(AllIPCTable.Fields.MAINGROUP,
                            rec.get(IPCRecord.Field.MAINGROUP));

                        if (!rec.get(IPCRecord.Field.SUBGROUP).isEmpty()) {
                          tfc.setValue(AllIPCTable.Fields.SUBGROUP,
                              rec.get(IPCRecord.Field.SUBGROUP));
                        }
                      }
                    }
                  }
                }

                try {
                  dataWriter.addContent(tfc);
                } catch (final SQLException ex) {
                  throw new RuntimeException(ex);
                }
              });
        }

        // IPC count per document table
        try (final IPCPerDocumentTable.Writer dataWriter =
                 new IPCPerDocumentTable.Writer(db.getConnection())) {
          numberOfIpcsPerDoc.entrySet().stream()
              .forEach(e -> {
                final TableFieldContent tfc = new TableFieldContent(ipcPerDoc);

                tfc.setValue(IPCPerDocumentTable.Fields.CODES, e.getKey());
                tfc.setValue(IPCPerDocumentTable.Fields.COUNT, e.getValue());

                try {
                  dataWriter.addContent(tfc);
                } catch (final SQLException ex) {
                  throw new RuntimeException(ex);
                }
              });
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
     * Target directory.
     */
    @Option(name = "-out", metaVar = "<file>",
        required = false, usage = "If specified, all data will be written as " +
        "SQLite database to the given file")
    File targetDb;

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

      if (this.targetDb != null) {
        if (this.targetDb.exists()) {
          LOG.error("Database file '{}' exist.", this.targetDb);
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }
}
