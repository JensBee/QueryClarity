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
package de.unihildesheim.lucene.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.index.CachedIndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.TextTable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeMap;
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
   * Print data to console as formatted table.
   */
  private final TextTable txtTbl;

  /**
   * Standard input for reading commands.
   */
  private final BufferedReader br = new BufferedReader(
          new InputStreamReader(System.in));

  /**
   * Current command entered by the user. Tokenized at spaces.
   */
  private String[] command = new String[]{""};

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
   * Prints the command help to {@link System#out}.
   */
  private void printHelp() {
    final List<String> commands = new ArrayList<>(2);
    commands.add("* listmodels [start] [count]- "
            + "list [count=100] models by document-id starting at [start=0].");
    commands.add("* listterms [start] [count]- "
            + "list [count=100] terms starting at [start=0].");
    commands.add("* showmodel [docid] - shows a model "
            + "for the given document id.");
    commands.add("* showtermdata [prefix] [key] [docId] [term] - "
            + "shows all known term-data. Keys are case sensitive!");
    commands.add("* showtermdatavalues [start] [count] - "
            + "show values of stored term-data.");
    commands.add("* showtermdatakeys - shows all known term-data keys.");
    commands.add("* status - show cache status data.");
    for (String cmd : commands) {
      System.out.println(cmd);
    }
  }

  /**
   * Get a part of the entered command as integer value, if available.
   *
   * @param i Position in input
   * @param defaultValue Default value
   * @return Value, or null if there was none
   */
  private Integer getIntParam(final int i, final Integer defaultValue) {
    Integer ret = defaultValue;
    if (this.command.length >= (i + 1)) {
      try {
        ret = Integer.parseInt(this.command[i]);
      } catch (NumberFormatException ex) {
        this.out.println("Expected an integer value at " + i + ".");
      }
    }
    return ret;
  }

  /**
   * Get a part of the entered command as long value, if available.
   *
   * @param i Position in input
   * @param defaultValue Default value
   * @return Value, or null if there was none
   */
  private Long getLongParam(final int i, final Long defaultValue) {
    Long ret = defaultValue;
    if (this.command.length >= (i + 1)) {
      try {
        ret = Long.parseLong(this.command[i]);
      } catch (NumberFormatException ex) {
        this.out.println("Expected a long value at " + i + ".");
      }
    }
    return ret;
  }

  /**
   * Get a part of the entered command as string value, if available.
   *
   * @param i Position in input
   * @param defaultValue Default value
   * @return Value, or null if there was none
   */
  private String getParam(final int i, final String defaultValue) {
    String ret = defaultValue;
    if (this.command.length >= i) {
      ret = this.command[i];
    }
    return ret;
  }

  /**
   * Reads a new command entered by the user.
   *
   * @param prompt Command prompt message to display
   */
  private void readCommand(final String prompt) {
    boolean quit = false;

    while (!quit) {
      System.out.print(prompt + ": ");

      try {
        this.command = br.readLine().trim().split(" ");
      } catch (IOException ioe) {
        System.out.println("IO error trying to read your name!");
        System.exit(1);
      }

      if ("?".equals(this.command[0])) {
        printHelp();
      } else if ("status".equals(this.command[0])) {
        cmdStatus();
      } else if (this.command[0].matches("^listmodels.*")) {
        cmdListModels();
      } else if (this.command[0].matches("^showmodel.*")) {
        cmdShowModel();
      } else if ("showtermdatakeys".equals(this.command[0])) {
        cmdShowTermDataKeys();
      } else if ("showtermdatavalues".equals(this.command[0])) {
        cmdShowTermDataValues();
      } else if (this.command[0].matches("^showtermdata.*")) {
        cmdShowTermData();
      } else if (this.command[0].matches("^listterms.*")) {
        cmdListTerms();
      } else {
        if ("q".equals(this.command[0])) {
          System.out.println("Quit.");
          quit = true;
        } else {
          System.out.println("Unknown command '" + this.command[0]
                  + "'. Type <q> to quit or <?> for help.");
        }
      }
    }
    System.exit(0);
  }

  /**
   * Calls the command prompt with default values.
   */
  private void readCommand() {
    readCommand("Command");
  }

  /**
   * Shows term data filtered by key elements entered by the user.
   */
  private void cmdShowTermData() {
    BTreeMap<Fun.Tuple4<String, String, Integer, BytesWrap>, Object> dataMap
            = this.dataProv.debugGetTermDataMap();

    String prefix = getParam(1, null);
    String key = getParam(2, null);
    Integer docId = getIntParam(3, null);
    String term = getParam(4, null);

    if (term != null) {
      System.out.println("Termdata for prefix=" + prefix + " key=" + key
              + " docId=" + docId + " term=" + term + ".");
      dataMap.get(Fun.t4(prefix, key, docId,
              new BytesWrap(new BytesRef(term))));
    } else if (docId != null) {
      System.out.println("Termdata for prefix=" + prefix + " key=" + key
              + " docId=" + docId + ".");
      Iterator<BytesWrap> tfIt = Fun.filter(dataMap.navigableKeySet(),
              prefix, key, docId).iterator();
      while (tfIt.hasNext()) {
        BytesWrap bw = tfIt.next();
        System.out.println("prefix=" + prefix + " key=" + key + " term="
                + BytesWrapUtil.bytesWrapToString(bw) + " data=" + dataMap.
                get(Fun.t4(prefix, key, docId, bw)));
      }
    } else if (key != null) {
      System.out.
              println("Termdata for prefix=" + prefix + " key=" + key + ".");
      Iterator<Fun.Tuple4<String, String, Integer, BytesWrap>> dataIt
              = dataMap.
              keySet().iterator();
      while (dataIt.hasNext()) {
        final Fun.Tuple4<String, String, Integer, BytesWrap> t4 = dataIt.
                next();
        if (t4.a.equals(prefix) && t4.b.equals(key)) {
          System.out.println("prefix=" + prefix + " key=" + key + " docId="
                  + t4.c + " term=" + BytesWrapUtil.
                  bytesWrapToString(t4.d) + " data=" + dataMap.get(t4));
        }
      }
    } else if (prefix != null) {
      System.out.println("Termdata for prefix=" + prefix + ".");
      Iterator<Fun.Tuple4<String, String, Integer, BytesWrap>> dataIt
              = dataMap.
              keySet().iterator();
      while (dataIt.hasNext()) {
        final Fun.Tuple4<String, String, Integer, BytesWrap> t4 = dataIt.
                next();
        if (t4.a.equals(prefix)) {
          System.out.println("prefix=" + prefix + " key=" + t4.b + " docId="
                  + t4.c + " term=" + BytesWrapUtil.bytesWrapToString(t4.d)
                  + " data=" + dataMap.get(t4));
        }
      }
    } else {
      System.out.println("All termdata.");

      for (Entry<Fun.Tuple4<String, String, Integer, BytesWrap>, Object> entry
              : dataMap.entrySet()) {
        final Fun.Tuple4<String, String, Integer, BytesWrap> t4 = entry.
                getKey();
        System.out.println("prefix=" + t4.a + " key=" + t4.b + " docId="
                + t4.c + " term=" + BytesWrapUtil.bytesWrapToString(t4.d)
                + " data=" + entry.getValue());
      }
    }
  }

  private void cmdShowTermDataValues() {
    Collection<Object> dataValues = this.dataProv.debugGetTermDataMap().
            values();

    Long start = getLongParam(1, 0L);
    Long amount = getLongParam(2, 100L);

    Iterator<Object> valuesIt = dataValues.iterator();
    int digitLength = String.valueOf(Math.max(start, start + amount)).length();
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting terms..");
    while (valuesIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        System.out.printf("[%" + digitLength + "d] value=%s\n", itemCounter,
                valuesIt.next());
      } else {
        valuesIt.next();
      }
    }
  }

  /**
   * Show all known prefix + key term-data.
   */
  private void cmdShowTermDataKeys() {
    Iterator<Fun.Tuple4<String, String, Integer, BytesWrap>> dataIt
            = this.dataProv.debugGetTermDataMap().keySet().iterator();

    System.out.println("Gathering keys. This may take some time.");

    final Map<String, Map<String, AtomicLong>> prefixData = new HashMap();
    int prefixSize = 0;
    int keySize = 0;
    int countSize = 0;

    while (dataIt.hasNext()) {
      final Fun.Tuple4<String, String, Integer, BytesWrap> t4 = dataIt.next();
      Map<String, AtomicLong> keyData;
      // check, if prefix is known
      if (prefixData.containsKey(t4.a)) {
        keyData = prefixData.get(t4.a);
      } else {
        keyData = new HashMap<>();
        prefixData.put(t4.a, keyData);
      }
      // update key
      if (keyData.containsKey(t4.b)) {
        keyData.get(t4.b).incrementAndGet();
      } else {
        keyData.put(t4.b, new AtomicLong(0));
      }

      // get print sizes
      if (keyData.get(t4.b).toString().length() > countSize) {
        countSize = keyData.get(t4.b).toString().length();
      }
      if (t4.a.length() > prefixSize) {
        prefixSize = t4.a.length();
      }
      if (t4.b.length() > keySize) {
        keySize = t4.b.length();
      }
    }

    // create table layout
    final String[] tblColumns = new String[]{"prefix", "key", "count"};
    final int[] tblCellWidths = this.txtTbl.getCellWidths(tblColumns,
            new int[]{prefixSize, keySize, countSize});
    this.txtTbl.setDefaultRowFormat(new String[]{"%s", "%s", "%d"});
    this.txtTbl.setCellWidths(tblColumns, tblCellWidths);
    this.txtTbl.header("TermData", tblColumns);

    for (Entry<String, Map<String, AtomicLong>> entryPrefix : prefixData.
            entrySet()) {
      boolean first = true;
      for (Entry<String, AtomicLong> entryKey : entryPrefix.getValue().
              entrySet()) {
        if (first) {
          this.txtTbl.row(new Object[]{entryPrefix.getKey(),
            entryKey.getKey(), entryKey.getValue().longValue()});
          first = false;
        } else {
          this.txtTbl.row(new Object[]{"", entryKey.getKey(), entryKey.
            getValue().longValue()});
        }
      }
    }
    this.txtTbl.hLine();
  }

  /**
   * Show data from a document model.
   */
  private void cmdShowModel() {
    Integer docId = getIntParam(1, null);

    if (docId == null) {
      this.out.println("No document id specified.");
      return;
    }

    final DocumentModel docModel = this.dataProv.getDocumentModel(docId);

    int maxTermLen = 0;
    int maxValLen = 0;
    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      final String bwString = BytesWrapUtil.bytesWrapToString(entry.getKey());
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
      this.txtTbl.row(new Object[]{
        BytesWrapUtil.bytesWrapToString(entry.getKey()), entry.getValue()});
    }
    this.txtTbl.hLine();
  }

  /**
   * List terms from cache.
   */
  private void cmdListTerms() {
    Long start = getLongParam(1, 0L);
    Long amount = getLongParam(2, 100L);

    List<BytesWrap> terms = new ArrayList<>((int) (long) amount);
    Iterator<BytesWrap> termsIt = this.dataProv.getTermsIterator();
    int termCharLength = 0;
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting terms..");
    while (termsIt.hasNext() && showCounter <= amount) {
      if (itemCounter++ >= start && showCounter++ <= amount) {
        final BytesWrap term = termsIt.next();
        final String termString = BytesWrapUtil.bytesWrapToString(term);
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
      this.txtTbl.row(new Object[]{start + i,
        BytesWrapUtil.bytesWrapToString(terms.get(i)), termFreq.get(i),
        rTermFreq.get(i)});
    }
    this.txtTbl.hLine();
  }

  /**
   * List models from cache.
   */
  private void cmdListModels() {
    Long start = getLongParam(1, 0L);
    Long amount = getLongParam(2, 100L);

    List<Integer> docIds = new ArrayList<>((int) (long) amount);
    Iterator<Integer> docIdIt = this.dataProv.getDocIdIterator();
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
  private void cmdStatus() {
    final Map<String, Object> data = new HashMap<>(3);

    data.put("Document models", this.dataProv.getDocModelCount());
    data.put("Total term-frequency", this.dataProv.getTermFrequency());
    data.put("Unique terms", this.dataProv.getTermsCount());

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

    readCommand();
  }

}
