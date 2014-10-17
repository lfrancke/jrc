package jrc;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public abstract class CellCalculator<T> {

  public static final int MIN_LAT = -90;
  public static final int MAX_LAT = 90;
  public static final int MIN_LON = -180;
  public static final int MAX_LON = 180;
  private long maxLonCell;
  private long maxLatCell;
  private double cellSize;

  public long getMaxLonCell() {
    return maxLonCell;
  }

  public long getMaxLatCell() {
    return maxLatCell;
  }

  public abstract T getCellEnvelope(long cell);

  public double getCellSize() {
    return cellSize;
  }

  public void setCellSize(double cellSize) {
    this.cellSize = cellSize;
    maxLonCell = (int) Math.floor((2 * MAX_LON) / cellSize);
    maxLatCell = (int) Math.floor((2 * MAX_LAT) / cellSize);
  }

  public long toCellId(double latitude, double longitude) throws HiveException {
    if (latitude < MIN_LAT || latitude > MAX_LAT || longitude < MIN_LON || longitude > MAX_LON) {
      throw new HiveException("Invalid coordinates");
    } else {
      long la = getLatitudeId(latitude, cellSize);
      long lo = getLongitudeId(longitude, cellSize);
      return Math.min(Math.max(la + lo, 0), maxLatCell * maxLonCell - 1);
    }
  }

  private long getLatitudeId(double latitude, double cellSize) {
    return new Double(Math.floor((latitude + MAX_LAT) / cellSize) * maxLonCell).longValue();
  }

  private long getLongitudeId(double longitude, double cellSize) {
    return new Double(Math.floor((longitude + MAX_LON) / cellSize)).longValue();
  }

}
