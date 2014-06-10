/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
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

import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.kohsuke.args4j.Option;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;

/**
 * Database tools.
 *
 * @author Jens Bertram
 */
public final class DbTools
    extends CliBase {

  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Constructor initializing meta information.
   */
  private DbTools() {
    super("Database tool collection.", "Tool collection for persistent data " +
        "storage files.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String[] args) {
    new DbTools().runMain(args);
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   */
  private void runMain(final String[] args) {
    parse(this.cliParams, args);

    this.cliParams.checkCommand();

    if (this.defaultCliParams.printHelp) {
      System.out.println("Command help '" + this.cliParams.command + "':");
      switch (StringUtils.lowerCase(this.cliParams.command)) {
        case "compact":
          System.out.println("Run compaction on the database.");
          break;
      }
    }

    this.cliParams.checkDbFile();

    switch (StringUtils.lowerCase(this.cliParams.command)) {
      case "compact":
        runCompact();
        break;
    }
  }

  /**
   * Run compaction on the current database.
   */
  private void runCompact() {
    final DBMaker dbMkr = DBMaker.newFileDB(this.cliParams.dbFile)
        .strictDBGet();
    if (this.cliParams.useCompression) {
      dbMkr.compressionEnable();
    }
    final DB db = dbMkr.make();
    System.out
        .println("Running compaction on '" + this.cliParams.dbFile + "'.");
    final TimeMeasure tm = new TimeMeasure().start();
    db.compact();
    System.out.println(
        "Compaction finished after " + tm.stop().getTimeString() + ".");
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Database name.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-f", aliases = "--file", metaVar = "NAME",
        required = true, usage = "Database file (without '.p' or '.t').")
    File dbFile;

    /**
     * True, if compression should be enabled.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-useCompression", required = false,
        usage = "Use database compression. Must be specified, " +
            "if the database was created using compression.")
    boolean useCompression;

    /**
     * Command to execute.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-c", aliases = "--command", metaVar = "(compact)",
        required = true,
        usage = "Command to execute. Use '-command-help' for details.")
    String command;

    /**
     * Empty constructor to allow access from parent class.
     */
    Params() {
    }

    /**
     * Checks, if the database file exist.
     */
    void checkDbFile() {
      if (!this.dbFile.exists()) {
        System.err
            .println("Error: Database file '" + this.dbFile + "' not found.");
        System.exit(1);
      }
    }

    /**
     * Check command parameters
     */
    void checkCommand() {
      // check command
      switch (StringUtils.lowerCase(this.command)) {
        case "compact":
          break;
        default:
          System.err.println("Error: Unknown command '" + this.command + "'.");
          System.exit(1);
      }
    }
  }
}
