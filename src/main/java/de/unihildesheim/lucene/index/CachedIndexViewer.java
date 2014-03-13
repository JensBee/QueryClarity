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
package de.unihildesheim.lucene.index;

import asg.cliche.CLIException;
import asg.cliche.Command;
import asg.cliche.Param;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.TextTable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.mapdb.Fun;

/**
 * Simple utility to view the contents of the {@link CachedIndexDataProvider}s
 * cache.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class CachedIndexViewer {

  /**
   * CLI-parameter specifying the storage id to load.
   */
  @Parameter(names = "-storageId", description
          = "Storage id for CachedIndexDataProvider", required = true)
  private String storageId;

  /**
   * CLI-parameter specifying the storage id to load.
   */
  @Parameter(names = "-command", description
          = "Single command to run and exit", required = false, variableArity
          = true)
  private List<String> runCommand = new ArrayList<String>();

  /**
   * Print data to console as formatted table.
   */
  private final TextTable txtTbl;

  /**
   * Cached data storage to browse.
   */
  private CachedIndexDataProvider dataProv;

  /**
   * Output stream to write to.
   */
  private final PrintStream out;

  /**
   * Private constructor for tool class.
   */
  private CachedIndexViewer() {
    this.out = System.out;
    this.txtTbl = new TextTable(this.out);
  }

  /**
   * Main function.
   *
   * @param args the command line arguments
   * @throws IOException Thrown on low-level I/O errors from Lucene index
   */
  public static void main(final String[] args) throws IOException {
    final CachedIndexViewer civ = new CachedIndexViewer();
    final JCommander jc = new JCommander(civ);
    try {
      jc.parse(args);
    } catch (ParameterException ex) {
      System.out.println(ex.getMessage() + "\n");
      jc.usage();
      System.exit(1);
    }
    civ.start();
  }

  /**
   * List known prefixes for external term-data.
   */
  @Command(description = "List known prefixes for external term-data.")
  public void listKnownPrefixes() {
    for (String prefix : this.dataProv.debugGetKnownPrefixes()) {
      System.out.println(prefix);
    }
  }

  @Command(description = "Show externally stored document-term data "
          + "by prefix and document-id.")
  public void showExternalTermData(
          @Param(name = "prefix", description = "External prefix to use.")
          final String prefix,
          @Param(name = "docId", description = "Document-id.")
          final Integer docId) {
    Map<Fun.Tuple2<String, String>, Object> map = this.dataProv.
            debugGetPrefixMap(prefix);
    if (map == null) {
      System.out.println("No data with prefix '" + prefix + "' found.");
      return;
    }

    Iterator<Entry<Fun.Tuple2<String, String>, Object>> entriesIt
            = map.entrySet().iterator();
    while (entriesIt.hasNext()) {
      final Entry<Fun.Tuple2<String, String>, Object> entry
              = entriesIt.next();
      if (entry.getKey().a.startsWith(docId.toString())) {
        System.out.printf("key={%s, %s} "
                + "value={%s}\n", entry.getKey().a, entry.
                getKey().b, entry.getValue());
      }
    }
  }

  /**
   * Show externally stored document-term data by prefix.
   *
   * @param prefix Prefix to use
   * @param start Offset to start at
   * @param amount Number of items to show
   */
  @Command(description = "Show externally stored document-term data "
          + "by prefix.")
  public void showExternalTermData(
          @Param(name = "prefix", description = "External prefix to use.")
          final String prefix,
          @Param(name = "start", description = "Index to start at.")
          final long start,
          @Param(name = "amount", description = "Number of items to show.")
          final long amount) {
    Map<Fun.Tuple2<String, String>, Object> map = this.dataProv.
            debugGetPrefixMap(prefix);
    if (map == null) {
      System.out.println("No data with prefix '" + prefix + "' found.");
      return;
    }

    Iterator<Entry<Fun.Tuple2<String, String>, Object>> entriesIt
            = map.entrySet().iterator();

    int digitLength = String.valueOf(Math.max(start, start + amount)).length();
    long itemCounter = 0;
    long showCounter = 1;
    while (entriesIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        final Entry<Fun.Tuple2<String, String>, Object> entry
                = entriesIt.next();
        System.out.printf("[%" + digitLength + "d] key={%s, %s} "
                + "value={%s}\n", itemCounter, entry.getKey().a, entry.
                getKey().b, entry.getValue());
      } else {
        entriesIt.next();
      }
    }
  }

  /**
   * List the given amount of items from base document-term storage, starting
   * at the given index.
   *
   * @param start Start index
   * @param amount Number of items to show
   */
  @Command(description = "Show base data stored for documents-terms.")
  public void showTermDataValues(
          @Param(name = "start", description = "Index to start at.")
          final long start,
          @Param(name = "amount", description = "Number of items to show.")
          final long amount) {
    Iterator<Entry<Fun.Tuple3<Integer, String, BytesWrap>, Object>> entriesIt
            = this.dataProv.debugGetInternalTermDataMap().entrySet().
            iterator();

    int digitLength = String.valueOf(Math.max(start, start + amount)).length();
    long itemCounter = 0;
    long showCounter = 1;
    while (entriesIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        final Entry<Fun.Tuple3<Integer, String, BytesWrap>, Object> entry
                = entriesIt.next();
        System.out.printf("[%" + digitLength + "d] key={%d, %s, %s} "
                + "value={%s}\n", itemCounter, entry.getKey().a, entry.
                getKey().b, entry.getKey().c, entry.getValue());
      } else {
        entriesIt.next();
      }
    }
  }

  /**
   * Show data for a document model with a given id.
   *
   * @param docId Document-id
   */
  @Command(description = "Show document model data.")
  public void showDocModel(
          @Param(name = "docId", description = "Document Id.")
          final int docId) {
    final DocumentModel docModel = this.dataProv.getDocumentModel(docId);

    int maxTermLen = 0;
    int maxValLen = 0;
    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      final String bwString = entry.getKey().toString();
      if (bwString.length() > maxTermLen) {
        maxTermLen = bwString.length();
      }
      if (entry.getValue().toString().length() > maxValLen) {
        maxValLen = entry.getValue().toString().length();
      }
    }

    // create table layout
    final String[] tblColumns = new String[]{"term", "value"};
    final int[] tblCellWidths = this.txtTbl.getCellWidths(tblColumns,
            new int[]{maxTermLen, maxValLen});
    this.txtTbl.setDefaultRowFormat(new String[]{"%s", "%d"});
    this.txtTbl.setCellWidths(tblColumns, tblCellWidths);
    this.txtTbl.header("DocId: " + docId, tblColumns);

    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      this.txtTbl.row(new Object[]{entry.getKey(), entry.getValue()});
    }
    this.txtTbl.hLine();
  }

  /**
   * List the given amount of terms from cache, starting at the given index.
   *
   * @param start Start index
   * @param amount Number of items to show
   */
  @Command(description = "List terms from cache.")
  public void listTerms(
          @Param(name = "start", description = "Index to start at.")
          final long start,
          @Param(name = "amount", description = "Number of items to show.")
          final long amount) {
    List<BytesWrap> terms = new ArrayList<>((int) (long) amount);
    Iterator<BytesWrap> termsIt = this.dataProv.getTermsIterator();
    int termCharLength = 0;
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting terms..");
    while (termsIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        final BytesWrap term = termsIt.next();
        final String termString = term.toString();
        // nice print
        if (termString.length() > termCharLength) {
          termCharLength = termString.length();
        }
        terms.add(term);
      } else {
        termsIt.next();
      }
    }

    List<Long> termFreq = new ArrayList<>((int) (long) amount);
    int tfDigitLength = 0;
    System.out.println("getting frequency data..");
    for (BytesWrap term : terms) {
      final Long termFrequency = this.dataProv.getTermFrequency(term);
      // nice print
      if (String.valueOf(termFrequency).length() > tfDigitLength) {
        tfDigitLength = String.valueOf(termFrequency).length();
      }
      termFreq.add(termFrequency);
    }

    List<Double> rTermFreq = new ArrayList<>((int) (long) amount);
    int rtfDigitLength = 0;
    System.out.println("getting relative frequency data..");
    for (BytesWrap term : terms) {
      final Double rTermFrequency = this.dataProv.getRelativeTermFrequency(
              term);
      // nice print
      if (rTermFrequency.toString().length() > rtfDigitLength) {
        rtfDigitLength = rTermFrequency.toString().length();
      }
      rTermFreq.add(rTermFrequency);
    }

    // create table layout
    final String[] tblColumns = new String[]{"#", "term", "frequency",
      "rel. frequency"};
    final int[] tblCellWidths = this.txtTbl.getCellWidths(tblColumns,
            new int[]{Long.valueOf(start + amount - 1).
              toString().length(), termCharLength, tfDigitLength,
              rtfDigitLength});
    this.txtTbl.setDefaultRowFormat(new String[]{"%s", "%s", "%d", "%f"});
    this.txtTbl.setCellWidths(tblColumns, tblCellWidths);
    this.txtTbl.header("Terms " + start + " - " + (start + amount - 1),
            tblColumns);

    for (int i = 0; i < terms.size(); i++) {
      this.txtTbl.row(new Object[]{start + i, terms.get(i), termFreq.get(i),
        rTermFreq.get(i)});
    }
    this.txtTbl.hLine();
  }

  /**
   * List document models. Starting at the given index, listing the specified
   * number of entries.
   *
   * @param start Start index
   * @param amount Number of items to show
   */
  @Command(description = "List document models from cache.")
  public void listModels(
          @Param(name = "start", description = "Index to start at.")
          final long start,
          @Param(name = "amount", description = "Number of items to show.")
          final long amount) {
    List<Integer> docIds = new ArrayList<>((int) (long) amount);
    Iterator<Integer> docIdIt = this.dataProv.getDocumentIdIterator();
    int idDigitLength = 0;
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting models..");
    while (docIdIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        final Integer docId = docIdIt.next();
        // nice print
        if (docId.toString().length() > idDigitLength) {
          idDigitLength = docId.toString().length();
        }
        docIds.add(docId);
      } else {
        docIdIt.next();
      }
    }

    List<Long> termFreq = new ArrayList<>((int) (long) amount);
    int tfDigitLength = 0;
    System.out.println("getting frequency data..");
    for (Integer docId : docIds) {
      final DocumentModel docModel = this.dataProv.getDocumentModel(docId);
      // nice print
      if (String.valueOf(docModel.termFrequency).length() > tfDigitLength) {
        tfDigitLength = String.valueOf(docModel.termFrequency).length();
      }
      termFreq.add(docModel.termFrequency);
    }

    // create table layout
    final String[] tblColumns = new String[]{"#", "id", "term-frequency"};
    final int[] tblCellWidths = this.txtTbl.getCellWidths(tblColumns,
            new int[]{Long.valueOf(start + amount - 1).
              toString().length(), idDigitLength, tfDigitLength});
    this.txtTbl.setDefaultRowFormat(new String[]{"%s", "%d", "%d"});
    this.txtTbl.setCellWidths(tblColumns, tblCellWidths);
    this.txtTbl.header("Models " + start + " - " + (start + amount - 1),
            tblColumns);

    for (int i = 0; i < docIds.size(); i++) {
      this.txtTbl.row(new Object[]{start + i, docIds.get(i),
        termFreq.get(i)});
    }
    this.txtTbl.hLine();
  }

  /**
   * Show cache status info.
   */
  @Command(description = "Show cache status info")
  public void status() {
    final Map<String, Object> data = new HashMap<>(3);

    data.put("Document models", this.dataProv.getDocumentCount());
    data.put("Total term-frequency", this.dataProv.getTermFrequency());
    data.put("Unique terms", this.dataProv.getUniqueTermsCount());

    int titleLength = 0;
    int dataLength = 0;
    for (Entry<String, Object> dataEntry : data.entrySet()) {
      if (dataEntry.getKey().length() > titleLength) {
        titleLength = dataEntry.getKey().length();
      }
      if (dataEntry.getValue().toString().length() > dataLength) {
        dataLength = dataEntry.getValue().toString().length();
      }
    }

    // create table layout
    final String[] tblColumns = new String[]{"Property", "Value"};
    final int[] tblCellWidths = this.txtTbl.getCellWidths(tblColumns,
            new int[]{titleLength, dataLength});
    this.txtTbl.setDefaultRowFormat(new String[]{"%s", "%s"});
    this.txtTbl.setCellWidths(tblColumns, tblCellWidths);
    this.txtTbl.header("Status", tblColumns);

    for (Entry<String, Object> dataEntry : data.entrySet()) {
      this.txtTbl.row(new Object[]{dataEntry.getKey(), dataEntry.getValue().
        toString()});
    }
    this.txtTbl.hLine();
  }

  /**
   * Quit the instance.
   */
  @Command(description = "Quit.")
  public void quit() {
    System.exit(0);
  }

  /**
   * Start the viewer.
   *
   * @throws IOException Thrown on low-level I/O errors related to Lucene
   * index
   */
  private void start() throws IOException {
    // get the data storage instance
    this.dataProv = new CachedIndexDataProvider(storageId);

    if (!dataProv.tryGetStoredData()) {
      System.out.println("IndexDataProvider has no stored data.");
      System.exit(1);
    }

    Shell shell = ShellFactory.createConsoleShell("cmd", "CachedIndexViewer",
            this);
    if (!this.runCommand.isEmpty()) {
      final String[] command = this.runCommand.toArray(
              new String[this.runCommand.size()]);
      try {
        shell.processLine(StringUtils.join(command, " "));
      } catch (CLIException ex) {
        ex.printStackTrace();
      }
    } else {
      shell.commandLoop();
    }
  }
}
