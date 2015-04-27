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

import de.unihildesheim.iw.storage.sql.MetaTable;
import de.unihildesheim.iw.storage.sql.Table;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.scoringData.ScoringDataDB;
import de.unihildesheim.iw.storage.sql.scoringData.TermScoringTable;
import de.unihildesheim.iw.storage.sql.termData.TermDataDB;
import de.unihildesheim.iw.storage.sql.termData.TermsTable;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

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
   * Target database for scoring data.
   */
  private ScoringDataDB scoreDb;

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
  public static void main(final String... args)
      throws SQLException, ClassNotFoundException {
    new ExtractTerms().runMain(args);
    Runtime.getRuntime().exit(0); // required to trigger shutdown-hooks
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

    // table manager instance: Source database with term data
    try (final TermDataDB termDb = new TermDataDB(this.cliParams.dbSource)) {

      if (termDb.hasTerms()) {
        // number of all terms in table
        final long termCount = termDb.getNumberOfTerms();
        // number of terms per bin
        final long binWidth = Math.floorDiv(termCount, this.cliParams.bins);

        // fields queries from source table
        final String fields = TermsTable.Fields.TERM.toString() + ',' +
            TermsTable.Fields.LANG + ',' +
            TermsTable.Fields.DOCFREQ_REL + ',' +
            TermsTable.Fields.DOCFREQ_ABS;
        // prepared statement to query terms from source table
        final PreparedStatement pickStmt = termDb.getConnection()
            .prepareStatement("SELECT " + fields + " from " +
                TermsTable.TABLE_NAME +
                " order by " + TermsTable.Fields.DOCFREQ_REL +
                " limit " + binWidth + " offset ?");

        // table manager instance: Target database with scoring data
        try (final ScoringDataDB scoringDb =
                 new ScoringDataDB(this.cliParams.dbTarget)) {
          // data table
          final TermScoringTable scoringTable = new TermScoringTable();
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
              final Collection<Integer> samples = createSamples(
                  this.cliParams.binSize, (int) binWidth);
              Collections.sort(new ArrayList<>(samples));

              if (LOG.isDebugEnabled()) {
                LOG.debug("Samples: {}", samples.size());
              }

              int count = 0;
              while (!samples.isEmpty() && rs.next()) {
                if (samples.contains(++count)) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("pick {}",
                        (binWidth * (long) binNo) + (long) count);
                  }
                  // columns are defined in fields variable
                  scoringWriter.addContent(new TableFieldContent(scoringTable)
                      .setValue(TermScoringTable.Fields.TERM, rs.getString(1))
                      .setValue(TermScoringTable.Fields.LANG, rs.getString(2))
                      .setValue(TermScoringTable.Fields.DOCFREQ_REL,
                          rs.getString(3))
                      .setValue(TermScoringTable.Fields.DOCFREQ_ABS,
                          rs.getString(4))
                      .setValue(TermScoringTable.Fields.BIN, binNo + 1));
                  samples.remove(count);
                }
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

        if (LOG.isDebugEnabled()) {
          LOG.debug("terms={} bins={} binsize={}",
              termCount, this.cliParams.bins, binWidth);
        }
      }
    }
  }

  /**
   * Create a list of random samples to get.
   *
   * @param amount Amount of random values to get
   * @param max Maximum value allowed (inclusive)
   * @return {@code amount} number of random numbers between {@code 0} and
   * {@code max} (each inclusive)
   */
  private static Collection<Integer> createSamples(
      final int amount, final int max) {
    final Collection<Integer> samples = new HashSet<>(amount);
    while (samples.size() < amount) {
      samples.add(RandomValue.getInteger(0, max));
    }
    return samples;
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Source database file file with term-dump data.
     */
    @Option(name = "-dumpdb", metaVar = "FILE", required = true,
        usage = "SQLite database with term-dump data.")
    File dbSource;

    /**
     * Target database file file.
     */
    @Option(name = "-scoredb", metaVar = "FILE", required = true,
        usage = "Target SQLite database with scoring data. Will be " +
            "created, if missing.")
    File dbTarget;

    /**
     * Document frequency threshold.
     */
    @Option(name = "-threshold", metaVar = "float", required = false,
        usage = "Document frequency threshold (relative). If this is exceeded" +
            " a term will be treated as being too common (means gets skipped)" +
            ". Default: 0")
    Double threshold = 0d;

    /**
     * Number of ranges (bins) to create to pick terms from.
     */
    @Option(name = "-bins", metaVar = "number", required = false,
        usage = "Number of ranges (bins) to create for picking terms. " +
            "Default: 5.")
    Integer bins = 5;

    /**
     * Number of terms to sample per bin.
     */
    @Option(name = "-binsize", metaVar = "number", required = false,
        usage = "Number of term to sample per bin. Default: 50.")
    Integer binSize = 50;

    /**
     * Bins to pick values from..
     */
    @Option(name = "-picks", metaVar = "1-[bins]", required = false,
        usage = "Ranges to pick terms from. Default: 1,3,5.")
    Integer[] picks = {1, 3, 5};

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
      if (this.picks.length <= 0) {
        throw new IllegalArgumentException("Number of picks must be >0.");
      }
      for (final int pick : this.picks) {
        if (pick <= 0) {
          throw new IllegalArgumentException("Picks must be >0.");
        }
        if (pick > this.bins) {
          throw new IllegalArgumentException("Pick " + pick +
              " exceeds number of bins (" + this.bins + ')');
        }
      }
      if (!this.dbSource.exists() || !this.dbSource.isFile()) {
        throw new IllegalStateException(
            "Source database " + this.dbSource +
                " does not exist or is a not a database file.");
      }
    }
  }
}
