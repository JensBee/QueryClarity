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

import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.Table;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.scoringData.ScoringDataDB;
import de.unihildesheim.iw.storage.sql.scoringData.TermScoringTable;
import de.unihildesheim.iw.storage.sql.termData.TermDataDB;
import de.unihildesheim.iw.storage.sql.termData.TermsTable;
import de.unihildesheim.iw.storage.sql.termData.TermsTable.FieldsOptional;
import de.unihildesheim.iw.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Commandline utility to extract terms from term dumps created with {@link
 * DumpTermData} from the Lucene index.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class ExtractTerms
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ExtractTerms.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Default private constructor passing a description to {@link CliBase}.
   */
  private ExtractTerms() {
    super("Extract terms from term-dump for scoring.",
        "Extract terms from a term-dump database and prepare a scoring " +
            "database.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown, if connection to the database has failed
   * FilteredDirectoryReader} instance has failed
   */
  public static void main(@NotNull final String... args)
      throws SQLException, ClassNotFoundException {
    new ExtractTerms().runMain(args);
    Runtime.getRuntime().exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Check if terms-table has an IPC-Code column and if the existance of this
   * column is required (IPC-Code specified on command-line). Throws an
   * Exception, if table has no IPC-Code column and an IPC-Code filter is
   * specified on command-line.
   *
   * @param termDb Database
   * @return True, if table has IPC-Codes
   * @throws SQLException Thrown on low-level SQL-errors
   */
  private boolean checkIPC(@NotNull final TermDataDB termDb)
      throws SQLException {
    final boolean hasIPCField = termDb.hasTableField(TermsTable.TABLE_NAME,
        FieldsOptional.IPC);

    // stop, if a IPC filter was given, but there are no IPC-codes stored
    if (!hasIPCField && this.cliParams.ipcRec != null) {
      throw new IllegalStateException("IPC filter requested, but no " +
          "IPC-codes are stored in the database.");
    }
    return hasIPCField;
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown, if connection to the database has failed
   * FilteredDirectoryReader} instance has failed
   */
  @SuppressWarnings({"UnnecessarilyQualifiedInnerClassAccess",
      "ObjectAllocationInLoop"})
  private void runMain(final String... args)
      throws SQLException, ClassNotFoundException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    LOG.info("Reading term-data from '{}'.", this.cliParams.dbSource);
    LOG.info("Writing scoring-data to '{}'.", this.cliParams.dbTarget);

    // use relative document-frequency threshold?
    final boolean useThreshold = this.cliParams.threshold > 0d;

    // normalize some parameters
    final CharSequence langName = StringUtils.lowerCase(this.cliParams.lang);
    final CharSequence fieldName = StringUtils.lowerCase(this.cliParams.field);
    final String ipcName;
    if (this.cliParams.ipcRec != null) {
      ipcName = this.cliParams.ipcRec
          .toFormattedString(this.cliParams.sep);
    } else {
      ipcName = "";
    }

    // table manager instance: Source database with term data
    try (final TermDataDB termDb = new TermDataDB(this.cliParams.dbSource)) {
      if (termDb.hasTerms()) {
        // Should we include IPC-data?
        final boolean includeIPC = checkIPC(termDb) &&
            (this.cliParams.ipcRec != null);

        // number of all terms in table
        final Statement preCheckStmt = termDb.getConnection().createStatement();
        String preCheckSQL =
            "select count(*) from " + TermsTable.TABLE_NAME +
                " where " + TermsTable.Fields.LANG + "='" + langName +
                "' and " + TermsTable.Fields.FIELD + "='" + fieldName + '\'';
        if (useThreshold) {
          preCheckSQL += " and " + TermsTable.Fields.DOCFREQ_REL +
              " >= " + this.cliParams.threshold;
        }
        if (includeIPC) {
          preCheckSQL += " and " + TermsTable.FieldsOptional.IPC + " like '" +
              ipcName + "%'";
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("PreCheckSQL: {}", preCheckSQL);
        }

        // pre-check, if there are any terms we can process
        long termCount = 0;
        if (preCheckStmt.execute(preCheckSQL)) {
          final ResultSet rsPreCheck = preCheckStmt.getResultSet();
          if (rsPreCheck.next()) {
            termCount = rsPreCheck.getLong(1);
          }
        }

        if (termCount <= 0L) {
          LOG.warn("No results returned from database.");
        }

        // number of terms per bin
        final long binWidth = Math.floorDiv(termCount, this.cliParams.bins);

        // check, if there are enough terms to sample per bin
        final int finalBinSize;
        if (binWidth < (long)this.cliParams.binSize) {
          // reduce the number of terms to sample to the number of total
          // terms available per bin
          finalBinSize = (int) binWidth;
          LOG.warn("Not enough terms to create {} samples per bin. " +
              "Number of samples reduced to {} samples per bin.",
              this.cliParams.binSize, finalBinSize);
        } else {
          finalBinSize = this.cliParams.binSize;
        }

        // fields queries from source table
        final String fields =
            TermsTable.Fields.TERM.toString() + ',' +
                TermsTable.Fields.LANG + ',' +
                TermsTable.Fields.DOCFREQ_REL + ',' +
                TermsTable.Fields.DOCFREQ_ABS + ',' +
                TermsTable.Fields.FIELD +
                (includeIPC ? "," + TermsTable.FieldsOptional.IPC : "");

        // prepared statement to query terms from source table
        final String querySQL =
            "SELECT " + fields +
                " from " + TermsTable.TABLE_NAME +
                " where " + TermsTable.Fields.LANG + "='" + langName + '\'' +
                (useThreshold ?
                    " and " + TermsTable.Fields.DOCFREQ_REL +
                        " >= " + this.cliParams.threshold : "") +
                " and " + TermsTable.Fields.FIELD + "='" + fieldName + '\'' +
                (includeIPC ?
                    " and " + TermsTable.FieldsOptional.IPC + " like '" +
                        ipcName + "%'" : "") +
                " order by " + TermsTable.Fields.DOCFREQ_REL +
                " limit " + binWidth + " offset ?";
        if (LOG.isDebugEnabled()) {
          LOG.debug("querySQL: {}", querySQL);
        }
        final PreparedStatement pickStmt = termDb.getConnection()
            .prepareStatement(querySQL);

        if (LOG.isDebugEnabled()) {
          LOG.debug("terms={} bins={} binsize={}",
              termCount, this.cliParams.bins, binWidth);
        }

        // table manager instance: Target database with scoring data
        try (final ScoringDataDB scoringDb =
                 new ScoringDataDB(this.cliParams.dbTarget)) {
          // data table
          final TermScoringTable scoringTable;
          if (includeIPC) {
            scoringTable = new TermScoringTable(
                TermScoringTable.FieldsOptional.IPC
            );
          } else {
            scoringTable = new TermScoringTable();
          }
          // meta table
          final Table metaTable = new MetaTable();

          scoringDb.createTables(scoringTable, metaTable);

          // write meta-data
          try (final MetaTable.Writer metaWriter =
                   new MetaTable.Writer(scoringDb.getConnection())) {
            metaWriter.addContent(new TableFieldContent(metaTable)
                .setValue(MetaTable.Fields.TABLE_NAME, scoringTable.getName())
                .setValue(MetaTable.Fields.CMD, StringUtils.join(args, " ")));
          }

          if (termCount > 0L) {
            LOG.info("Generating sample seed.");
            // range of numbers (0 to binWidth) to sample from
            final List<Integer> sampleRange =  Arrays.asList(
                IntStream.range(0, (int) binWidth)
                    .boxed().toArray(Integer[]::new));

            // write term scoring data
            try (final TermScoringTable.Writer scoringWriter =
                     new TermScoringTable.Writer(scoringDb.getConnection())) {
              // number of bins picked for scoring (and storing to target db)
              final int pickAmount = this.cliParams.picks.length;
              for (int pickIdx = 0; pickIdx < pickAmount; pickIdx++) {
                final int binNo = this.cliParams.picks[pickIdx] - 1;
                LOG.info("Bin-pick {}: bin={}({}) row {}-{}",
                    pickIdx, binNo + 1, binNo, binWidth * (long) binNo,
                    (binWidth * (long) binNo) + binWidth);

                pickStmt.setLong(1, binWidth * (long) binNo);
                final ResultSet rs = pickStmt.executeQuery();

                Collections.shuffle(sampleRange);
                final List<Integer> samples = new ArrayList<>(finalBinSize);
                for (int i=0; i<finalBinSize; i++) {
                  samples.add(sampleRange.get(i));
                }
                Collections.sort(samples);

                if (LOG.isDebugEnabled()) {
                  LOG.debug("Pick-idx {}", samples);
                }

                LOG.info("Picking {} from bin {}.", samples.size(), binNo + 1);

                Integer count = 0;
                while (!samples.isEmpty() && rs.next()) {
                  if (samples.contains(count)) {
                    if (LOG.isTraceEnabled()) {
                      LOG.trace("pick {}",
                          (binWidth * (long) binNo) + (long) count);
                    }
                    // columns are defined in fields variable
                    final TableFieldContent tfc =
                        new TableFieldContent(scoringTable)
                            .setValue(TermScoringTable.Fields.TERM,
                                rs.getString(1))
                            .setValue(TermScoringTable.Fields.LANG,
                                rs.getString(2))
                            .setValue(
                                TermScoringTable.Fields.DOCFREQ_REL,
                                rs.getString(3))
                            .setValue(
                                TermScoringTable.Fields.DOCFREQ_ABS,
                                rs.getString(4))
                            .setValue(
                                TermScoringTable.Fields.BIN,
                                binNo + 1)
                            .setValue(
                                TermScoringTable.Fields.FIELD,
                                rs.getString(5));
                    if (includeIPC) {
                      tfc.setValue(TermScoringTable.FieldsOptional.IPC,
                          rs.getString(6));
                    }
                    scoringWriter.addContent(tfc, false);
                    samples.remove(count);
                  }
                  count++;
                }
                if (!samples.isEmpty()) {
                  final String msg = "Not all samples retrieved. "
                      + samples.size() + " remaining unresolved.";
                  LOG.error(msg);
                  throw new IllegalStateException(msg);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Source database file file with term-dump data.
     */
    @Option(name = "-dumpdb", metaVar = "<database file>", required = true,
        usage = "SQLite database with term-dump data.")
    File dbSource;

    /**
     * Target database file file.
     */
    @Option(name = "-scoredb", metaVar = "<database file>", required = true,
        usage = "Target SQLite database with scoring data. Will be " +
            "created, if missing.")
    File dbTarget;

    /**
     * Document frequency threshold.
     */
    @Option(name = "-threshold", metaVar = "<float>", required = false,
        usage = "Document frequency threshold (relative). If this is exceeded" +
            " a term will be treated as being too common (means gets skipped)" +
            ". Default: 0.01")
    Double threshold = 0.01;

    /**
     * Number of ranges (bins) to create to pick terms from.
     */
    @Option(name = "-bins", metaVar = "<number>", required = false,
        usage = "Number of ranges (bins) to create for picking terms. " +
            "Default: 5.")
    Integer bins = 5;

    /**
     * Number of terms to sample per bin.
     */
    @Option(name = "-binsize", metaVar = "<number>", required = false,
        usage = "Number of term to sample per bin. Default: 50.")
    Integer binSize = 50;

    /**
     * Bins to pick values from..
     */
    @Option(name = "-picks", metaVar = "<1-[bins]>", required = false,
        handler = StringArrayOptionHandler.class,
        usage = "Ranges to pick terms from. Default: 1 3 5.")
    String[] pickList = {"1", "3", "5"};
    /**
     * Final validated picks, parsed to int from input string.
     */
    int[] picks;

    /**
     * Source field to process.
     */
    @Option(name = "-field", metaVar = "<source field name>", required = true,
        usage = "Process terms from the given field.")
    String field;

    /**
     * Source field to process.
     */
    @Nullable
    @Option(name = "-ipc", metaVar = "<(partial) IPC-Code>", required = false,
        usage = "Process terms from the given (partial) IPC-code only.")
    String ipc;
    /**
     * Final IPC-Record created from user input (if specified).
     */
    @Nullable
    IPCRecord ipcRec;

    /**
     * Default separator char.
     */
    @Option(name = "-grpsep", metaVar = "<separator char>",
        required = false,
        usage = "Char to use for separating main- and sub-group.")
    char sep = Parser.DEFAULT_SEPARATOR;

    /**
     * Language to process.
     */
    @Option(name = "-lang", metaVar = "<language>", required = true,
        usage = "Process for the defined language.")
    String lang;

    /**
     * Empty constructor to allow access from parent class.
     */
    Params() {
      // empty
    }

    /**
     * Check commandline parameters.
     */
    void check() {
      if (this.binSize <= 0) {
        throw new IllegalArgumentException(
            "Number of samples per bins must be >0.");
      }
      if (this.bins <= 0) {
        throw new IllegalArgumentException("Number of bins must be >0.");
      }

      // check bin picks
      final Collection<String> pickSet =
          new HashSet<>(Arrays.asList(this.pickList));
      if (pickSet.isEmpty()) {
        throw new IllegalArgumentException("Number of picks must be >0.");
      }
      this.picks = new int[pickSet.size()];
      int idx = 0;
      for (final String pickStr : pickSet) {
        final int pick;
        try {
          pick = Integer.parseInt(pickStr);
        } catch (final NumberFormatException e) {
          throw new IllegalArgumentException(
              "Picks must be >0. Got " + pickStr);
        }
        if (pick <= 0) {
          throw new IllegalArgumentException("Picks must be >0.");
        }
        if (pick > this.bins) {
          throw new IllegalArgumentException("Pick " + pick +
              " exceeds number of bins (" + this.bins + ')');
        }
        this.picks[idx++] = pick;
      }

      if (!this.dbSource.exists() || !this.dbSource.isFile()) {
        throw new IllegalStateException(
            "Source database " + this.dbSource +
                " does not exist or is a not a database file.");
      }
      if (this.ipc != null) {
        final Parser ipcParser = new Parser();
        ipcParser.separatorChar(this.sep);
        this.ipcRec = ipcParser.parse(this.ipc);
      }
    }
  }
}
