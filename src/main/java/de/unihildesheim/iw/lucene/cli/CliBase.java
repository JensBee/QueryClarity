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

package de.unihildesheim.iw.lucene.cli;

import org.kohsuke.args4j.ClassParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * @author Jens Bertram
 */
public abstract class CliBase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(CliBase.class);

  private final String header;
  private final String info;
  private final DefaultCliParams defaultCliParams;

  /**
   * Initialize the cli base class.
   *
   * @param newHeader Short header to print when class is loaded
   * @param newInfo Short info to tell what this cli-class does
   */
  protected CliBase(final String newHeader, final String newInfo) {
    this.header = newHeader;
    this.info = newInfo;
    this.defaultCliParams = new DefaultCliParams();
  }

  /**
   * Print the header message
   *
   * @param out Output stream
   */
  protected void printHeader(final PrintStream out) {
    out.println(this.header);
  }

  /**
   * Print the info message
   *
   * @param out Output stream
   */
  protected void printInfo(final PrintStream out) {
    out.println(this.info);
  }

  protected CmdLineParser parse(final Object bean, final String[] args) {
    final CmdLineParser parser = new CmdLineParser(this.defaultCliParams);
    final ClassParser beanOpts = new ClassParser();
    beanOpts.parse(bean, parser);

    printHeader(System.out);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException ex) {
      // succeeds in case help is requested
      this.defaultCliParams.printHelpAndExit(parser, System.out, 0, false);
      // succeeds, if there is an error
      System.err.println("Error: " + ex.getMessage());
      this.defaultCliParams.printHelpAndExit(parser, System.err, 1, true);
    }

    this.defaultCliParams.printHelpAndExit(parser, System.out, 0, false);
    return parser;
  }

  /**
   * Default commandline parameters shared by all instances.
   */
  private final class DefaultCliParams {
    @Option(name = "-h", aliases = "--help", usage = "Usage help",
        required = false)
    private boolean printHelp;

    /**
     * Prints a help string and exists, if requested by cli params.
     *
     * @param parser Parser to get a usage help string from
     * @param out Stream to print to
     * @param exitCode Code to exit with
     * @param force If true, printing is forces, regardless if it's requested by
     * cli param
     */
    protected void printHelpAndExit(final CmdLineParser parser,
        final PrintStream out, final int exitCode, final boolean force) {
      if (printHelp || force) {
        printInfo(out);
        parser.printUsage(out);
        System.exit(exitCode);
      }
    }
  }
}
