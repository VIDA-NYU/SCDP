package de.metanome.algorithms.singlecolumnprofiler;

import java.util.HashMap;
import java.util.Map;

import de.metanome.algorithms.singlecolumnprofiler.basic_statistic_values.*;

/**
 * Represents a basic statistic result.
 */
public class BasicStatistic {

  public static final String NAME_COLUMN_SEPARATOR = " - ";
  public static final String COLUMN_VALUE_SEPARATOR = ": ";
  public static final String STATISTIC_SEPARATOR = "; ";
  public String name = "";

  private static final long serialVersionUID = -8010850754433867718L;

  protected Map<String, BasicStatisticValue> statisticMap;

  /**
   * Exists for serialization.
   */
  protected BasicStatistic(String name) {
      this.name = name;
    this.statisticMap = new HashMap<>();
  }

  /**
   * Adds a statistic to the result
   * @param statisticName  the name of the statistic
   * @param statisticValue the value of the statistic
   */
  public void addStatistic(String statisticName, BasicStatisticValue statisticValue) {
    this.statisticMap.put(statisticName, statisticValue);
  }

  public Map<String, BasicStatisticValue> getStatisticMap() {
    return statisticMap;
  }

  /**
   * Sets the statistics of the result.
   * @param statisticMap the statistic map containing all statistics
   */
    public void setStatisticMap(Map<String, BasicStatisticValue> statisticMap) {
    this.statisticMap = statisticMap;
  }

  @Override
  public String toString() {
      String str = name + COLUMN_VALUE_SEPARATOR;

    for (Map.Entry<String, BasicStatisticValue> entry : this.statisticMap.entrySet()) {
      str += entry.getKey() + NAME_COLUMN_SEPARATOR + entry.getValue() + STATISTIC_SEPARATOR;
    }

    return str;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result;
    result = prime * result
      + ((this.statisticMap.isEmpty()) ? 0 : this.statisticMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BasicStatistic other = (BasicStatistic) obj;
    if (!this.statisticMap.equals(other.statisticMap)) {
      return false;
    }
    return true;
  }

}

