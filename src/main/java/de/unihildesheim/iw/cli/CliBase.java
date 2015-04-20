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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
public class CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(CliBase.class);
  /**
   * Object holding default commandline parameters.
   */
  final DefaultCliParams defaultCliParams;
  /**
   * Header banner for cli output.
   */
  private final String header;
  /**
   * Info banner for cli output.
   */
  private final String info;
  /**
   * True, if help was already printed.
   */
  private boolean helpPrinted;

  /**
   * Initialize the cli base class.
   *
   * @param newHeader Short header to print when class is loaded
   * @param newInfo Short info to tell what this cli-class does
   */
  protected CliBase(@NotNull final String newHeader, @NotNull final String
      newInfo) {
    this.header = newHeader;
    this.info = newInfo;
    this.defaultCliParams = new DefaultCliParams();
  }

  /**
   * Parse the commandline and print the help message.
   *
   * @param bean Bean containing cli params definitions
   * @param args Commandline parameters
   * @return Cli arguments parser instance
   */
  protected final CmdLineParser parseWithHelp(
      @Nullable final Object bean, @NotNull final String... args) {
    final CmdLineParser parser = parse(bean, args);

    if (!this.helpPrinted) {
      this.defaultCliParams.printHelpAndExit(parser, System.out, 0, false);
    }
    return parser;
  }

  protected final CmdLineParser parseWithHelp(@NotNull final String... args) {
    return parseWithHelp(null, args);
  }

  /**
   * Parse the commandline and print a help message, if requested. Also quits
   * the instance, if an error occurred.
   *
   * @param bean Bean containing cli params definitions
   * @param args Commandline parameters
   * @return Cli arguments parser instance
   */
  final CmdLineParser parse(
      @Nullable final Object bean, @NotNull final String... args) {
    final CmdLineParser parser = new CmdLineParser(this.defaultCliParams);
    if (bean != null) {
      new ClassParser().parse(bean, parser);
    }

    printHeader(System.out);

    try {
      parser.parseArgument(args);
    } catch (final CmdLineException ex) {
      if (this.defaultCliParams.printHelp) {
        // succeeds in case help is requested
        printInfo(System.out);
        parser.printUsage(System.out);
        this.helpPrinted = true;
      } else {
        // succeeds, if there is an error
        LOG.error(ex.getMessage());
        this.defaultCliParams.printHelpAndExit(parser, System.err, 1, true);
      }
    }

    return parser;
  }

  /**
   * Print the header message
   *
   * @param out Output stream
   */
  final void printHeader(@NotNull final PrintStream out) {
    out.println(this.header);
  }

  /**
   * Print the info message
   *
   * @param out Output stream
   */
  final void printInfo(@NotNull final PrintStream out) {
    out.println(this.info);
  }

  /**
   * Default commandline parameters shared by all instances.
   */
  final class DefaultCliParams {
    /**
     * True, if help message was requested.
     */
    @Option(name = "-h", aliases = "--help", usage = "Usage help",
        required = false)
    boolean printHelp;

    /**
     * Prints a help string and exists, if requested by cli params.
     *
     * @param parser Parser to get a usage help string from
     * @param out Stream to print to
     * @param exitCode Code to exit with
     * @param force If true, printing is forces, regardless if it's requested by
     * cli param
     */
    void printHelpAndExit(
        @NotNull final CmdLineParser parser,
        @NotNull final PrintStream out,
        final int exitCode, final boolean force) {
      if (this.printHelp || force) {
        printInfo(out);
        parser.printUsage(out);
        System.exit(exitCode);
      }
    }
  }
}
