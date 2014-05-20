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
package de.unihildesheim.iw.util;

import java.io.PrintStream;

/**
 * Utility class to create simple ASCII tables outputted to a {@link
 * PrintStream}.
 */
public final class TextTable {

  /**
   * Output stream to print to.
   */
  private final PrintStream out;
  /**
   * Global cell with definition.
   */
  private int[] cells = new int[0];
  /**
   * Flag indicating, if the global cells have been set.
   */
  private boolean hasCells;
  /**
   * Default string formatting for a row.
   */
  private String[] defaultRowFormat;
  /**
   * Flag indicating if a default row format is set.
   */
  private boolean hasRowFormat;

  /**
   * New text-table instance printing to the defined stream.
   *
   * @param newOut Stream to output the table data to
   */
  public TextTable(final PrintStream newOut) {
    this.out = newOut;
  }

  /**
   * Set the cell widths globally, taking the with of the column headers into
   * account.
   *
   * @param header Column headers
   * @param dataWidth With of each data cell
   */
  public void setCellWidths(final String[] header, final int[] dataWidth) {
    this.cells = new int[header.length];
    for (int i = 0; i < header.length; i++) {
      this.cells[i] = Math.max(header[i].length(), dataWidth[i]);
    }
    this.hasCells = true;
  }

  /**
   * Set the cell widths globally.
   *
   * @param cellWidths Array of cells specified by their widths
   */
  public void setCellWidth(final int[] cellWidths) {
    this.cells = cellWidths.clone();
    this.hasCells = true;
  }

  /**
   * Set a default format for printing rows.
   *
   * @param format Format definition for printf
   */
  public void setDefaultRowFormat(final String[] format) {
    this.defaultRowFormat = format.clone();
    this.hasRowFormat = true;
  }

  /**
   * Check, if cells are globally defined. Throws an exception, if it's not the
   * case.
   */
  private void hasCells() {
    if (!this.hasCells) {
      throw new IllegalArgumentException("Cells not specified.");
    }
  }

  /**
   * Check, if a default row format is defined. Throws an exception, if it's not
   * the case.
   */
  private void hasRowFormat() {
    if (!this.hasRowFormat) {
      throw new IllegalArgumentException("Row format not specified.");
    }
  }

  /**
   * Repeat a string a given amount of times.
   *
   * @param times How many times to repeat
   * @param string String to repeat
   * @return New string with the given string <tt>times</tt> repeated
   */
  private String rPrint(final int times, final String string) {
    if (times > 0) {
      return new String(new char[times]).replace("\0", string);
    } else {
      return "";
    }
  }

  /**
   * Get the with of the specified cells, taking spacing into account.
   *
   * @param newCells Cells to use for calculation
   * @return With used by the specified cells
   */
  private int getCellsWidth(final int... newCells) {
    int width = 0;
    for (final int newCell : newCells) {
      width += newCell + 2; // add spacing
    }
    return width;
  }

  /**
   * Print a horizontal table cell line for the given cells with vertical line
   * start and end markers for each cell.
   *
   * @param newCells Cells specified by their width
   */
  public void hLine(final int... newCells) {
    hLine(true, newCells);
  }

  /**
   * Same as {@link #hLine(int...)}, but uses the globally specified cells.
   */
  @SuppressWarnings({"ConfusingArrayVararg",
      "PrimitiveArrayArgumentToVariableArgMethod"})
  public void hLine() {
    hasCells();
    hLine(true, this.cells);
  }

  /**
   * Print a horizontal table cell line with optional vertical line markers at
   * start and end of each cell for the given cells.
   *
   * @param printMarkers If true, a vertical line marker will be printed for
   * each cell
   * @param newCells Cells specified by their width
   */
  public void hLine(final boolean printMarkers, final int... newCells) {
    boolean start;
    boolean end = false;

    for (int i = 0; i < newCells.length; i++) {
      start = i == 0;
      if (i + 1 == newCells.length) {
        end = true;
      }

      hLine(newCells[i], printMarkers, start, end);
    }
  }

  /**
   * Print a horizontal line of the given with with optional start and end
   * markers.
   *
   * @param width Width of the line
   * @param marker If true, a vertical line marker will be printed for each
   * cell
   * @param start If true, cell is first
   * @param end If true, cell is last
   */
  public void hLine(final int width, final boolean marker,
      final boolean start,
      final boolean end) {
    if (start) {
      this.out.print("+");
    }
    this.out.print("-" + rPrint(width, "-") + "-");
    if (marker || end) {
      this.out.print("+");
    } else {
      this.out.print("-");
    }
    if (end) {
      this.out.print("\n");
    }
  }

  /**
   * Print a horizontal line of the given width.
   *
   * @param width Width of the line
   */
  public void hLine(final int width) {
    hLine(width, false, true, true);
  }

  /**
   * Print the table header.
   *
   * @param title Table header
   * @param newCells Cells of the table
   */
  private void printHeader(final String title, final int... newCells) {
    @SuppressWarnings({"ConfusingArrayVararg",
        "PrimitiveArrayArgumentToVariableArgMethod"})
    int width = getCellsWidth(newCells);
    // fix spacing introduced by vertical lines
    if (newCells.length > 3) {
      width += newCells.length - 3;
    }
    this.out.printf("| %-" + width + "s |\n", title);
  }

  /**
   * Prints a table heading (title).
   *
   * @param title Title of the table
   * @param newCells Cell widths
   */
  @SuppressWarnings({"ConfusingArrayVararg",
      "PrimitiveArrayArgumentToVariableArgMethod"})
  public void header(final String title, final int... newCells) {
    hLine(false, newCells);
    printHeader(title, newCells);
    hLine(false, newCells);
  }

  /**
   * Same as {@link #header(java.lang.String, int...)}, but uses the globally
   * specified cells.
   *
   * @param title Title of the table
   */
  @SuppressWarnings({"ConfusingArrayVararg",
      "PrimitiveArrayArgumentToVariableArgMethod"})
  public void header(final String title) {
    hasCells();
    header(title, this.cells);
  }

  /**
   * Print a table header with following column titles.
   *
   * @param title Title of the table
   * @param columns Column titles
   * @param cellWidths Cell widths
   */
  @SuppressWarnings({"ConfusingArrayVararg",
      "PrimitiveArrayArgumentToVariableArgMethod"})
  public void header(final String title, final String[] columns,
      final int[] cellWidths) {
    hLine(false, cellWidths);
    printHeader(title, cellWidths);
    hLine(true, cellWidths);
    cHeader(columns, cellWidths);
    hLine(true, cellWidths);
  }

  /**
   * Same as {@link #header(java.lang.String, java.lang.String[], int[])}, but
   * uses the globally specified cells.
   *
   * @param title Title of the table
   * @param columns Column titles
   */
  public void header(final String title, final String[] columns) {
    hasCells();
    header(title, columns, this.cells);
  }

  /**
   * Prints a column heading line.
   *
   * @param columns Column titles
   * @param cellWidths Cell widths
   */
  public void cHeader(final String[] columns, final int[] cellWidths) {
    boolean start;
    boolean end = false;
    for (int i = 0; i < columns.length; i++) {
      start = i == 0;
      if (i + 1 == columns.length) {
        end = true;
      }

      if (start) {
        this.out.print("|");
      }
      this.out.printf(" %" + cellWidths[i] + "s |", columns[i]);
      if (end) {
        this.out.print("\n");
      }
    }
  }

  /**
   * Same as {@link #cHeader(java.lang.String[], int[])}, but uses the globally
   * specified cells.
   *
   * @param columns Column titles
   */
  public void cHeader(final String[] columns) {
    hasCells();
    cHeader(columns, this.cells);
  }

  /**
   * Calculate the real cell widths based on cell data and column headers.
   *
   * @param columns Column headers
   * @param dataWidth Cell data width
   * @return Array of table cell widths
   */
  public int[] getCellWidths(final String[] columns, final int[] dataWidth) {
    int[] cellWidths = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      cellWidths[i] = Math.max(columns[i].length(), dataWidth[i]);
    }
    return cellWidths;
  }

  /**
   * Print a data row.
   *
   * @param data Data to print
   * @param format Format specifier for each column as defined by printf
   */
  public void row(final Object[] data, final String[] format) {
    hasCells();
    for (int i = 0; i < this.cells.length; i++) {
      if (i == 0) {
        this.out.print("|");
      }
      this.out.printf(" " + format[i].replace("%", "%" + this.cells[i])
          + " |", data[i]);
    }
    this.out.print("\n");
  }

  /**
   * Same as {@link #row(java.lang.Object[], java.lang.String[])}, but uses the
   * globally defined row format.
   *
   * @param data Data to print in a row
   */
  public void row(final Object[] data) {
    hasCells();
    hasRowFormat();
    row(data, this.defaultRowFormat);
  }
}
