/*
 * Copyright (C) 2014 Jens Bertram
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
package de.unihildesheim.lucene.cli;

/**
 * Definitions for general commandline parameters.
 *
 * @author Jens Bertram
 */
public final class CliParams {

  /**
   * Parameter to specify the Lucene index directory.
   */
  public static final String INDEX_DIR_P = "-i";
  /**
   * Usage for specifying the Lucene index directory.
   */
  public static final String INDEX_DIR_U = "Path to Lucene index.";

  /**
   * Parameter to run a single shell command.
   */
  public static final String SHELL_EXEC_P = "-exec";
  /**
   * Usage for running a single shell command.
   */
  public static final String SHELL_EXEC_U = "Run a single command and exit.";

  /**
   * Private empty constructor for utility class.
   */
  private CliParams() {
    // empty
  }
}
