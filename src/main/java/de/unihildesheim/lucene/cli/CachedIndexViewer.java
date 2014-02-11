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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
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
   * Standard input for reading commands.
   */
  private final BufferedReader br = new BufferedReader(
          new InputStreamReader(System.in));

  /**
   * Used to tokenize commands.
   */
  private Scanner inputScan;

  /**
   * Cached data storage to browse.
   */
  private CachedIndexDataProvider dataProv;

  /**
   * Private constructor for tool class.
   */
  private CachedIndexViewer() {
    // empty for tool class
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
    commands.add("* showtermdatakeys - shows all known term-data keys.");
    commands.add("* status - show cache status data.");
    for (String cmd : commands) {
      System.out.println(cmd);
    }
  }

  /**
   * Reads a new command entered by the user.
   *
   * @param prompt Command prompt message to display
   */
  private void readCommand(final String prompt) {
    String command = "";
    boolean quit = false;

    while (!quit) {
      System.out.print(prompt + ": ");
      try {
        this.inputScan = new Scanner(br.readLine());
      } catch (IOException ioe) {
        System.out.println("IO error trying to read your name!");
        System.exit(1);
      }

      if (inputScan.hasNext()) {
        command = inputScan.next();
      } else {
        System.exit(0);
      }

      if ("?".equals(command)) {
        printHelp();
      } else if ("status".equals(command)) {
        cmdStatus();
      } else if (command.matches("^listmodels.*")) {
        cmdListModels();
      } else if (command.matches("^showmodel.*")) {
        cmdShowModel();
      } else if ("showtermdatakeys".equals(command)) {
        cmdShowTermDataKeys();
      } else if (command.matches("^showtermdata.*")) {
        cmdShowTermData();
      } else if (command.matches("^listterms.*")) {
        cmdListTerms();
      } else {
        if ("q".equals(command)) {
          System.out.println("Quit.");
          quit = true;
        } else {
          System.out.println("Unknown command '" + command
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
   * Repeat a string a given amount of times.
   *
   * @param times How many times to repeat
   * @param string String to repeat
   * @return New string with the given string <tt>times</tt> repeated
   */
  private String repeatPrint(final int times, final String string) {
    if (times > 0) {
      return new String(new char[times]).replace("\0", string);
    } else {
      return "";
    }
  }

  /**
   * Print a table cell line for the given cells to {@link System#out}.
   *
   * @param cells Cells specified by their width
   */
  private void printTableCellLine(final int... cells) {
    boolean start;
    boolean end = false;

    for (int i = 0; i < cells.length; i++) {
      start = i == 0;
      if (i + 1 == cells.length) {
        end = true;
      }

      if (start) {
        System.out.print("+");
      }
      System.out.print("-" + repeatPrint(cells[i], "-") + "-+");
      if (end) {
        System.out.print("\n");
      }
    }
  }

  /**
   * Shows term data filtered by key elements entered by the user.
   */
  private void cmdShowTermData() {
    BTreeMap<Fun.Tuple4<String, String, Integer, BytesWrap>, Object> dataMap
            = this.dataProv.debugGetTermDataMap();

    String prefix = null;
    String key = null;
    Integer docId = null;
    String term = null;

    if (inputScan.hasNext()) {
      try {
        prefix = inputScan.next();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [prefix] value.");
        return;
      }
    }
    if (inputScan.hasNext()) {
      try {
        key = inputScan.next();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [key] value.");
        return;
      }
    }
    if (inputScan.hasNext()) {
      try {
        docId = inputScan.nextInt();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [docId] value.");
        return;
      }
    }
    if (inputScan.hasNext()) {
      try {
        term = inputScan.next();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [term] value.");
        return;
      }
    }

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
                get(
                        Fun.t4(prefix, key, docId, bw)));
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
      Iterator<Fun.Tuple4<String, String, Integer, BytesWrap>> dataIt
              = dataMap.
              keySet().iterator();
      while (dataIt.hasNext()) {
        final Fun.Tuple4<String, String, Integer, BytesWrap> t4 = dataIt.
                next();
        System.out.println("prefix=" + t4.a + " key=" + t4.b + " docId="
                + t4.c
                + " term=" + BytesWrapUtil.bytesWrapToString(t4.d) + " data="
                + dataMap.get(t4));
      }
    }
  }

  /**
   * Show all known prefix + key term-data.
   */
  private void cmdShowTermDataKeys() {
    Map<Fun.Tuple4<String, String, Integer, BytesWrap>, Object> dataMap
            = this.dataProv.debugGetTermDataMap();

    System.out.println("Gathering keys. This may take some time.");

    final Map<String, Set<String>> prefixKeys = new HashMap<>();
    int prefixSize = 0;
    int keySize = 0;
    for (Fun.Tuple4<String, String, Integer, BytesWrap> t4 : dataMap.keySet()) {
      if (prefixKeys.containsKey(t4.a)) {
        prefixKeys.get(t4.a).add(t4.b);
      } else {
        final Set<String> keys = new HashSet<>();
        keys.add(t4.b);
        prefixKeys.put(t4.a, keys);
      }
      if (t4.a.length() > prefixSize) {
        prefixSize = t4.a.length();
      }
      if (t4.b.length() > keySize) {
        keySize = t4.b.length();
      }
    }

    final String prefixHeader = "prefix";
    final String keyHeader = "key";
    prefixSize = Math.max(prefixHeader.length(), prefixSize);
    keySize = Math.max(keyHeader.length(), keySize);

    final String kHeader = "TermData";
    printTableCellLine(keySize + prefixSize + 3);
    System.out.printf("| %" + (keySize + prefixSize + 3) + "s |\n", kHeader);
    printTableCellLine(keySize + prefixSize + 3);
    System.out.printf("| %" + prefixSize + "s | %" + keySize + "s |\n",
            prefixHeader, keyHeader);
    printTableCellLine(prefixSize, keySize);

    for (Entry<String, Set<String>> entry : prefixKeys.entrySet()) {
      boolean first = true;
      final Set<String> keySet = entry.getValue();
      for (String key : keySet) {
        if (first) {
          System.out.printf("| %" + prefixSize + "s | %" + keySize + "s |\n",
                  entry.getKey(), key);
          first = false;
        } else {
          System.out.printf("| %" + prefixSize + "s | %" + keySize + "s |\n",
                  "", key);
        }
      }
    }
    printTableCellLine(prefixSize, keySize);
  }

  /**
   * Show data from a document model.
   */
  private void cmdShowModel() {
    Integer docId;
    try {
      docId = inputScan.nextInt();
    } catch (InputMismatchException ex) {
      System.out.println("Invalid [docId] value.");
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

    printTableCellLine(maxTermLen + maxValLen + 3);
    System.out.printf("| %" + (maxTermLen + maxValLen + 3) + "s |\n",
            "DocId: " + docId);

    printTableCellLine(maxTermLen, maxValLen);
    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      System.out.printf("| %" + maxTermLen + "s | %" + maxValLen + "d |\n",
              BytesWrapUtil.bytesWrapToString(entry.getKey()),
              entry.getValue());
    }
    printTableCellLine(maxTermLen, maxValLen);
  }

  /**
   * List terms from cache.
   */
  private void cmdListTerms() {
    Long start;
    Long amount = 100L;

    try {
      start = inputScan.nextLong();
    } catch (InputMismatchException ex) {
      System.out.println("Invalid [start] value.");
      return;
    }
    if (inputScan.hasNext()) {
      try {
        amount = inputScan.nextLong();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [amount] value.");
        return;
      }
    }

    System.out.println("Start at " + start + " list " + amount);

    List<BytesWrap> terms = new ArrayList<>(Long.valueOf(amount).intValue());
    Iterator<BytesWrap> termsIt = this.dataProv.getTermsIterator();
    int termCharLength = 0;
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting terms..");
    while (termsIt.hasNext()) {
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

    List<Long> termFreq = new ArrayList<>(Long.valueOf(amount).intValue());
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

    List<Double> rTermFreq = new ArrayList<>(Long.valueOf(amount).intValue());
    int rtfDigitLength = 0;
    System.out.println("getting relative frequency data..");
    for (BytesWrap term : terms) {
      final Double rTermFrequency = this.dataProv.getRelativeTermFrequency(
              term);
      // nice print
      if (String.valueOf(rTermFrequency).length() > rtfDigitLength) {
        rtfDigitLength = String.valueOf(rTermFrequency).length();
      }
      rTermFreq.add(rTermFrequency);
    }

    // print table
    final String idxHeader = "#";
    final String idHeader = "term";
    final String tfHeader = "frequency";
    final String rtfHeader = "rel. frequency";
    int idxDigits = Math.max(idxHeader.length(), Long.valueOf(start + amount).
            toString().length());
    int idDigits = Math.max(idHeader.length(), termCharLength);
    int tfDigits = Math.max(tfHeader.length(), tfDigitLength);
    int rtfDigits = Math.max(rtfHeader.length(), rtfDigitLength);
    printTableCellLine(idxDigits, idDigits, tfDigits, rtfDigits);
    System.out.printf("| %" + idxDigits + "s | %" + idDigits + "s | %"
            + tfDigits + "s | %" + rtfDigits + "s |\n", idxHeader, idHeader,
            tfHeader, rtfHeader);
    printTableCellLine(idxDigits, idDigits, tfDigits, rtfDigits);
    for (int i = 0; i < terms.size(); i++) {
      System.out.printf("| %" + idxDigits + "s | %" + idDigits + "s | %"
              + tfDigits + "d | %" + rtfDigits + "f |\n", start + i,
              BytesWrapUtil.bytesWrapToString(terms.get(i)), termFreq.get(i),
              rTermFreq.get(i));
    }
    printTableCellLine(idxDigits, idDigits, tfDigits, rtfDigits);
  }

  /**
   * List models from cache.
   */
  private void cmdListModels() {
    Long start;
    Long amount = 100L;

    try {
      start = inputScan.nextLong();
    } catch (InputMismatchException ex) {
      System.out.println("Invalid [start] value.");
      return;
    }
    if (inputScan.hasNext()) {
      try {
        amount = inputScan.nextLong();
      } catch (InputMismatchException ex) {
        System.out.println("Invalid [amount] value.");
        return;
      }
    }

    System.out.println("Start at " + start + " list " + amount);

    List<Integer> docIds = new ArrayList<>(Long.valueOf(amount).intValue());
    Iterator<Integer> docIdIt = this.dataProv.getDocIdIterator();
    int idDigitLength = 0;
    long itemCounter = 0;
    long showCounter = 1;
    System.out.println("getting models..");
    while (docIdIt.hasNext()) {
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

    List<Long> termFreq = new ArrayList<>(Long.valueOf(amount).intValue());
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

    // print table
    final String idxHeader = "#";
    final String idHeader = "id";
    final String tfHeader = "term-frequency";
    int idxDigits = Math.max(idxHeader.length(), Long.valueOf(start + amount).
            toString().length());
    int idDigits = Math.max(idHeader.length(), idDigitLength);
    int tfDigits = Math.max(tfHeader.length(), tfDigitLength);
    printTableCellLine(idxDigits, idDigits, tfDigits);
    System.out.printf("| %" + idxDigits + "s | %" + idDigits + "s | %"
            + tfDigits + "s |\n", idxHeader, idHeader, tfHeader);
    printTableCellLine(idxDigits, idDigits, tfDigits);
    for (int i = 0; i < docIds.size(); i++) {
      System.out.printf("| %" + idxDigits + "s | %" + idDigits + "d | %"
              + tfDigits + "d |\n", start + i, docIds.get(i), termFreq.get(i));
    }
    printTableCellLine(idxDigits, idDigits, tfDigits);
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

    printTableCellLine(titleLength, dataLength);
    for (Entry<String, Object> dataEntry : data.entrySet()) {
      System.out.printf("| %" + titleLength + "s | %" + dataLength + "s |\n",
              dataEntry.getKey(), dataEntry.getValue().toString());
    }
    printTableCellLine(titleLength, dataLength);
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
