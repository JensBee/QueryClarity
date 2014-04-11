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
 * Definitions for general shell commands.
 *
 * @author Jens Bertram
 */
public class ShellCmds {

  /**
   * Short command to quit the shell.
   */
  public static final String QUIT_S = "q";
  /**
   * Long command to quit the shell.
   */
  public static final String QUIT_L = "quit";
  /**
   * Usage for quitting the shell.
   */
  public static final String QUIT_U = "Exit the shell.";

  /**
   * Private empty constructor for utility class.
   */
  private ShellCmds() {
    // empty
  }
}
