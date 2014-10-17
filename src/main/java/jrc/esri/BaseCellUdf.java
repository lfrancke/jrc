package jrc.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.SpatialReference;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class BaseCellUdf extends UDF {

  protected static final int MIN_LAT = -90;
  protected static final int MAX_LAT = 90;
  protected static final int MIN_LON = -180;
  protected static final int MAX_LON = 180;
  protected static final SpatialReference SPATIAL_REFERENCE = SpatialReference.create(4326);
  private final OperatorIntersects intersectsOperator =
    (OperatorIntersects) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersects);
  private long maxLonCell;
  private long maxLatCell;
  private Double cellSize;
  private final Envelope envelope = new Envelope();


  public long getMaxLonCell() {
    return maxLonCell;
  }

  public long getMaxLatCell() {
    return maxLatCell;
  }

  public OperatorIntersects getIntersectsOperator() {
    return intersectsOperator;
  }

  protected Envelope getCellEnvelope(long cell) {
    long row = cell / maxLonCell;
    long col = cell % maxLonCell;
    envelope.setCoords(MIN_LON + col * cellSize,
                       MIN_LAT + row * cellSize,
                       MIN_LON + col * cellSize + cellSize,
                       MIN_LAT + row * cellSize + cellSize);
    intersectsOperator.accelerateGeometry(envelope, SPATIAL_REFERENCE, Geometry.GeometryAccelerationDegree.enumHot);
    return envelope;
  }

  protected void setCellSize(double cellSize) {
    if (this.cellSize == null) {
      this.cellSize = cellSize;
      maxLonCell = (int) Math.floor((2 * MAX_LON) / cellSize);
      maxLatCell = (int) Math.floor((2 * MAX_LAT) / cellSize);
    }
  }

  protected long toCellId(double latitude, double longitude, double cellSize) throws HiveException {
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
