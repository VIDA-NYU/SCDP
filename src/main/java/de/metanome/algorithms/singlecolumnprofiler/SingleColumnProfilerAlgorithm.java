package de.metanome.algorithms.singlecolumnprofiler;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONObject;

import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValue;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueDouble;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueInteger;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueIntegerList;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueLong;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueString;
import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.BasicStatisticValueStringList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

public class SingleColumnProfilerAlgorithm {
    
  public static final int Numoftopk = 10;
  /**
   * If set to true, then no metric that require aggregation should be calculated (e.g., top-k frequent items, distinct values, ...)
   */
  protected boolean isNotAggregating = false;
  // general statistic
  protected String relationName;
  protected String fileName;
  protected BufferedReader reader;
  protected int numAttributes = 0;
  protected long NumofTuples = 0;
  protected List<String> columnNames = new ArrayList<String>();
  protected ObjectArrayList<ColumnMainProfile> columnsProfile;
  protected String outputPath;
  protected JSONObject jsonObj = new JSONObject();
  // statistic Names
  public final String NUMCOLUMN = "Number of Columns";
  public final String NUMTUPLE = "Number of Tuples";
  public final String COLUMNNAME = "Column Name";
  public final String NUMBEROFNULL = "Nulls";
  public final String PERCENTOFNULL = "Percentage of Nulls";
  public final String NUMBEROFDISTINCT = "Number of Distinct Values";
  public final String PERCENTODFISTINCT = "Percentage of Distinct Values";
  public final String DISTINCTVALUES = "Distinct Values";
  public final String VALUEDISTRIBUTION = "Value Distribution";
  public final String ENTROPY = "Entropy";
  public final String STRINGLENGTHDISTRIBUTION = "String Length Distribution";
  public final String TOPKITEM = "Top " + Numoftopk + " frequent items";
  public final String TOPKITEMFREQ = "Frequency Of Top " + Numoftopk + " Frequent Items";
  public final String DATATYPE = "Data Type";
  public final String LONGESTSTRING = "Longest String";
  public final String SHORTESTSTRING = "Shortest String";
  public final String MINSTRING = "Min String";
  public final String MAXSTRING = "Max String";
  public final String SEMANTICDATATYPE = "Symantic DataType";
  public final String MIN = "Min";
  public final String MAX = "Max";
  public final String AVG = "Avg.";
  public final String STDD = "Standard Deviation";
  // Delimiter used in CSV file
  BufferedWriter bufferWritter;

  // private static final String COMMA_DELIMITER = ",";
  // private static final String NEW_LINE_SEPARATOR = "\n";
  public void execute(String datasetName, String file) throws IOException {

    ////////////////////////////////////////////
    // THE DISCOVERY ALGORITHM LIVES HERE :-) //
    ////////////////////////////////////////////
    // just to generate my data
    // try{
    // File file =new File("generateschema.txt");
    //
    // //if file doesnt exists, then create it
    // if(!file.exists()){
    // file.createNewFile();
    // }
    //
    // //true = append file
    // FileWriter fileWritter = new FileWriter(file.getName(),true);
    // bufferWritter = new BufferedWriter(fileWritter);
    // }
    // catch (IOException e) {
    // //exception handling left as an exercise for the reader
    // }
    // =======================================================
    // step 1: initialisation
      relationName = datasetName;
      fileName = file;
    InitialiseColumnProfiles();

    // step 2: get data types
    getColumnsProfiles();

    // step 3: output
    // JSONObject General = new JSONObject();
    // General.put(NUMCOLUMN, columnNames.size());
    // General.put(NUMTUPLE, NumofTuples);
//    BasicStatistic gbs = new BasicStatistic(new ColumnIdentifier("*", relationName));
//    gbs.addStatistic(NUMCOLUMN, new BasicStatisticValueInteger(columnNames.size()));
//    gbs.addStatistic(NUMTUPLE, new BasicStatisticValueInteger(NumofTuples));
//    resultReceiver.receiveResult(gbs);
   for (int i = 0; i < columnNames.size(); i++) {
      // System.out.println(columnsProfile.get(i).toString());
      generateColumnStatistic(columnsProfile.get(i));
      columnsProfile.set(i,null);
      //System.out.println("");
    }
   System.out.print(jsonObj.toString(2) + "\n");
  }
  
  private BufferedReader readFile(String fileName) throws FileNotFoundException {
      return new BufferedReader(new FileReader(fileName));
  }

  private void InitialiseColumnProfiles() throws IOException {
      reader = readFile(fileName);
      
      String line = reader.readLine();
      
      // Header: assuming first line is the header
      String[] columnNames = Util.splitStr(line);
      for (int i = 0; i < columnNames.length; i++) {
          this.columnNames.add(columnNames[i]);
      }
      numAttributes = columnNames.length;
      
      line = reader.readLine();
    
    columnsProfile = new ObjectArrayList<>();
    // generate an initial profiles according to the first record
    if (line != null) {
        String[] firstRecord = Util.splitStr(line);
        
        if (firstRecord.length != numAttributes) {
            System.out.println("[Warning] First line has a different number of attributes than the header.");
            return;
        }
        
      NumofTuples++;
      // for each column
      for (int i = 0; i < columnNames.length; i++) {
        ColumnMainProfile profile = new ColumnMainProfile(columnNames[i]);
        String currentColumnvalue = firstRecord[i];

        // data type even if null the type is NA
        profile.setDataType(DataTypes.getDataType(currentColumnvalue));

        // null value
        if (currentColumnvalue == null)
          profile.increaseNumNull();
        else {
          // longest and shortest string
          profile.setLongestString(currentColumnvalue);
          profile.setShortestString(currentColumnvalue);

          // frequency
          if (!this.isNotAggregating) profile.addValueforfreq(currentColumnvalue);
          // profile.addValueforlengdist(currentColumnvalue.length());
          ////////////////
          // rest values
          /////////////////////

          // max min sum
          if (DataTypes.isNumeric(profile.getDataType())) {
            double doubleValue = Util.getnumberfromstring(currentColumnvalue);
            profile.setMax(doubleValue);
            profile.setMin(doubleValue);
            profile.setSum(doubleValue);

          }


        }
        columnsProfile.add(i, profile);

      }

    }

  }

  private void getColumnsProfiles() throws IOException {
    // first pass
    //////////////////////////////////////////////
    String line = reader.readLine();
    // for each tuple
    while (line != null) {
        String[] currentRecord = Util.splitStr(line);
        
        if (currentRecord.length != numAttributes) {
            System.out.println("[Warning] Line has a different number of attributes than the header.");
            line = reader.readLine();
            continue;
        }
      // read a tuple
      NumofTuples++;
      // for each column in a tuple verify the data type and update if new data type detected
      for (int i = 0; i < currentRecord.length; i++)
        if (currentRecord[i] == "")
          columnsProfile.get(i).increaseNumNull();
        else
          columnsProfile.get(i).updateColumnProfile(currentRecord[i]);
      line = reader.readLine();
    }
    reader.close();
    //////////////////////////////////////////
    // add the special types of string
    for (int i = 0; i < columnsProfile.size(); i++) {
      if (columnsProfile.get(i).getDataType() == DataTypes.mySTRING)
        if (columnsProfile.get(i).getLongestString().length() > 255)
          columnsProfile.get(i).setDataType(DataTypes.myTEXT);
        else if (columnsProfile.get(i).getDataType() == DataTypes.mySTRING
            && columnsProfile.get(i).getLongestString().length() == columnsProfile.get(i)
                .getShortestString().length()
            && columnsProfile.get(i).getNumNull() == 0
            && DataTypes.isUUID(columnsProfile.get(i).getLongestString())
            && DataTypes.isUUID(columnsProfile.get(i).getShortestString())

        )
          columnsProfile.get(i).setDataType(DataTypes.myUUID);

    }
    // columnsProfile.get(i).setCalculatedFields(NumofTuples);
    // try {
    //
    // columnsProfile.get(i).writeMapToCsv(outputPath, relationName.replaceAll(".csv", ""),
    // columnsProfile.get(i).getColumnName());
    // columnsProfile.get(i).setFreq(null);
    ///////////////////////////////////////////////////////////////////
    // bufferWritter.write(relationName.replaceAll(".csv", ""));
    // bufferWritter.write(COMMA_DELIMITER);
    // bufferWritter.write(columnsProfile.get(i).getColumnName());
    // bufferWritter.write(COMMA_DELIMITER);
    // bufferWritter.write(columnsProfile.get(i).getDataType());
    // bufferWritter.write(COMMA_DELIMITER);
    // if(columnsProfile.get(i).getNumNull()==0)
    // bufferWritter.write("NOTNULL");
    // else
    // bufferWritter.write("NULL");
    // bufferWritter.write(NEW_LINE_SEPARATOR);
    // bufferWritter.flush();
    //////////////////////////////////////////////////////////////////
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
    /////////////////////////////////////
    // add the second pass value
    reader = readFile(fileName);
    line = reader.readLine();
    line = reader.readLine();
    while (line != null) {
      // read a tuple
        String[] currentRecord = Util.splitStr(line);
        
        if (currentRecord.length != numAttributes) {
            System.out.println("[Warning] Line has a different number of attributes than the header.");
            line = reader.readLine();
            continue;
        }
      // for each column in a tuple
      for (int i = 0; i < currentRecord.length; i++)
        if (currentRecord[i] != null) {
          columnsProfile.get(i).updateColumnProfilesecondpass(currentRecord[i], NumofTuples);
        }
      
      line = reader.readLine();

    }
    reader.close();
    for (int i = 0; i < columnsProfile.size(); i++)
      columnsProfile.get(i)
          .setStdDev(Math.sqrt((columnsProfile.get(i).getStdDev() / (NumofTuples - 1))));
  }



  private void generateColumnStatistic(ColumnMainProfile cs) {

    BasicStatistic bs = new BasicStatistic(cs.getColumnName());

    // for all with string
    bs.addStatistic(NUMTUPLE, new BasicStatisticValueLong(NumofTuples));
    bs.addStatistic(NUMBEROFNULL, new BasicStatisticValueLong(cs.getNumNull()));
    bs.addStatistic(PERCENTOFNULL,
        new BasicStatisticValueLong(cs.getNumNull() * 100 / NumofTuples));
    if (!this.isNotAggregating) {
      Object2IntMap<String> valueFrequencies = cs.getFreq();
      bs.addStatistic(NUMBEROFDISTINCT, new BasicStatisticValueInteger(valueFrequencies.size()));
      bs.addStatistic(PERCENTODFISTINCT,
              new BasicStatisticValueInteger((int) (valueFrequencies.size() * 100 / NumofTuples)));
      double redundancy = 0d;
      IntIterator intIterator = valueFrequencies.values().iterator();
      int size = valueFrequencies.values().size();
      int done = 0;
      while (done < size) {
          done++;
          int valueFrequency = intIterator.nextInt();
          redundancy += valueFrequency * Math.log(valueFrequency);
      }
      double entropy = NumofTuples == 0 ?
              0d :
              (Math.log(NumofTuples) - redundancy / NumofTuples) / Math.log(2);
      bs.addStatistic(ENTROPY, new BasicStatisticValueDouble(entropy));
    }


    // if (cs.getDistinctValues() != null) column.put(DISTINCTVALUES, cs.getDistinctValues());
    // if(cs.getFreq()!=null) column.put(VALUEDISTRIBUTION, Util.mapToJson(cs.getFreq()));

    // just for strings
    if (cs.getDataType() == DataTypes.mySTRING) {
      String stringwithlength =
          cs.getDataType() + "[" + Util.roundUp(cs.getLongestString().length(), 16) + "]";
      bs.addStatistic(DATATYPE, new BasicStatisticValueString(stringwithlength));
      if (cs.getLongestString() != null)
        bs.addStatistic(LONGESTSTRING, new BasicStatisticValueString(cs.getLongestString()));
      if (cs.getShortestString() != null)
        bs.addStatistic(SHORTESTSTRING, new BasicStatisticValueString(cs.getShortestString()));
      if (!this.isNotAggregating) {
        ObjectSortedSet<String> valueFrequencies = new ObjectRBTreeSet<>(cs.getFreq().keySet());
        bs.addStatistic(MINSTRING, new BasicStatisticValueString(valueFrequencies.first()));
        bs.addStatistic(MAXSTRING, new BasicStatisticValueString(valueFrequencies.last()));
      }

      if (cs.getSemantictype() != null && cs.getSemantictype() != DataTypes.UNKOWN)
        bs.addStatistic(SEMANTICDATATYPE, new BasicStatisticValueString(cs.getSemantictype()));
      // if(cs.getLengthdist()!=null) column.put(STRINGLENGTHDISTRIBUTION,Util.mapToJsonIntegerKey(
      // cs.getLengthdist()));

    } else {
      // all types not string
      bs.addStatistic(DATATYPE, new BasicStatisticValueString(cs.getDataType()));

      // just numbers
      if (DataTypes.isNumeric(cs.getDataType())) {
        if (cs.getMin() != null)
          bs.addStatistic(MIN, new BasicStatisticValueDouble(cs.getMin()));

        if (cs.getMax() != null)
          bs.addStatistic(MAX, new BasicStatisticValueDouble(cs.getMax()));

        bs.addStatistic(AVG, new BasicStatisticValueDouble(cs.getSum() / NumofTuples));

        Double stdev = cs.getStdDev();
        if (!stdev.equals(Double.NaN))
          bs.addStatistic(STDD, new BasicStatisticValueDouble(cs.getStdDev()));

      }

    }


    // all
    if (!this.isNotAggregating) {
      Object2IntMap<String> valueFrequencies = cs.getFreq();
      TreeMap<String, Integer> topk = (TreeMap<String, Integer>) Util.getTopK(valueFrequencies, SingleColumnProfilerAlgorithm.Numoftopk);
      if(topk!=null){
        bs.addStatistic(TOPKITEM,
                new BasicStatisticValueStringList(new ArrayList<>(topk.keySet())));
        bs.addStatistic(TOPKITEMFREQ,
                new BasicStatisticValueIntegerList(new ArrayList<>(topk.values())));
      }
    }
    
    Map<String, BasicStatisticValue> stats = bs.getStatisticMap();
    
    JSONObject columnJson = new JSONObject();
    
    for (Map.Entry<String, BasicStatisticValue> entry : stats.entrySet()) {
        columnJson.put(entry.getKey(), entry.getValue());
      }
    jsonObj.put(cs.getColumnName(), columnJson);
    
  }


}
