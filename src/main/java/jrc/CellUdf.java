package jrc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

public class CellUdf extends UDF {

  private static final Log LOG = LogFactory.getLog(CellUdf.class);
  public static final int MIN_LAT = -90;
  public static final int MAX_LAT = 90;
  public static final int MIN_LON = -180;
  public static final int MAX_LON = 180;

  private final Map<Integer, Envelope> cellEnvelopes = Maps.newHashMap();
  private final OperatorIntersects operator =
    (OperatorIntersects) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersects);

  public CellUdf() {
    SpatialReference spatialReference = SpatialReference.create(4326);
    for (int i = 0; i < 2 * MAX_LON * 2 * MAX_LAT; i++) {
      int row = i / (2 * MAX_LON);
      int col = i % (2 * MAX_LON);
      Envelope envelope = new Envelope(MIN_LON + col, MIN_LAT + row, MIN_LON + col + 1, MIN_LAT + row + 1);
      cellEnvelopes.put(i, envelope);
      operator.accelerateGeometry(envelope, spatialReference, Geometry.GeometryAccelerationDegree.enumHot);
    }

  }

  public List<Integer> evaluate(BytesWritable b) throws HiveException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    // 1. Create bounding box
    OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(b);
    if (ogcGeometry == null) {
      LOG.warn("Geometry is null");
      return null;
    }

    if (ogcGeometry.isEmpty()) {
      LOG.warn("Geometry is empty");
      return null;
    }

    if (!"Polygon".equals(ogcGeometry.geometryType()) && !"MultiPolygon".equals(ogcGeometry.geometryType())) {
      LOG.warn("Geometry is not a polygon: " + ogcGeometry.geometryType());
      return null;
    }

    Envelope envBound = new Envelope();
    ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
    if (envBound.isEmpty()) {
      LOG.warn("Envelope is empty");
      return null;
    }

    // 2. Get all candidate cells
    Set<Integer> cellsEnclosedBy =
      getCellsEnclosedBy(envBound.getYMin(), envBound.getYMax(), envBound.getXMin(), envBound.getXMax());
    LOG.debug("Potential cells found: " + cellsEnclosedBy.size());

    cellsEnclosedBy = getCellIntersects(ogcGeometry, cellsEnclosedBy);
    LOG.debug("Actual cells found: " + cellsEnclosedBy.size());

    return Lists.newArrayList(cellsEnclosedBy);
  }

  private Set<Integer> getCellIntersects(OGCGeometry ogcGeometry, Iterable<Integer> cellsEnclosedBy) {
    operator.accelerateGeometry(ogcGeometry.getEsriGeometry(),
                                ogcGeometry.getEsriSpatialReference(),
                                Geometry.GeometryAccelerationDegree.enumHot);

    Set<Integer> cells = Sets.newHashSet();
    for (Integer cell : cellsEnclosedBy) {
      if (intersects(cell, ogcGeometry)) {
        cells.add(cell);
      }
    }
    return cells;
  }

  private boolean intersects(int cell, OGCGeometry ogcGeometry) {
    return operator.execute(ogcGeometry.getEsriGeometry(),
                            cellEnvelopes.get(cell),
                            ogcGeometry.getEsriSpatialReference(),
                            null);
  }

  private static int toCellId(double latitude, double longitude) throws HiveException {

    if (latitude < MIN_LAT || latitude > MAX_LAT || longitude < MIN_LON || longitude > MAX_LON) {
      throw new HiveException("Invalid coordinates");
    } else {
      int la = getLatitudeId(latitude);
      int lo = getLongitudeId(longitude);
      return Math.min(Math.max(la + lo, 0), 2 * MAX_LAT * 2 * MAX_LON - 1);
    }
  }

  private static int getLatitudeId(double latitude) {
    return new Double(Math.floor(latitude + MAX_LAT)).intValue() * 2 * MAX_LON;
  }

  private static int getLongitudeId(double longitude) {
    return new Double(Math.floor(longitude + MAX_LON)).intValue();
  }

  private static Set<Integer> getCellsEnclosedBy(double minLat, double maxLat, double minLon, double maxLon)
    throws HiveException {
    minLat = Math.max(MIN_LAT, minLat);
    maxLat = Math.min(MAX_LAT, maxLat);
    minLon = Math.max(MIN_LON, minLon);
    maxLon = Math.min(MAX_LON, maxLon);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Establishing cells enclosed by: " + minLon + ":" + minLat + "   " + maxLon + ":" + maxLat);
    }

    int lower = toCellId(minLat, minLon);
    int upper = toCellId(maxLat, maxLon);

    // TODO: Simplify by just creating a 1 cell buffer

    if (LOG.isDebugEnabled()) {
      LOG.debug("Unprocessed cells: " + lower + " -> " + upper);
    }

    if (maxLon % 1 == 0 && maxLon < MAX_LON) {
      LOG.debug("Max Longitude lies on a boundary");
      upper += 1;
    }
    if (maxLat % 1 == 0 && maxLat < MAX_LAT) {
      LOG.debug("Max Latitude lies on a boundary");
      upper += 2 * MAX_LON;
    }

    if (minLon % 1 == 0 && minLon > MIN_LON) {
      LOG.debug("Min Longitude lies on a boundary");
      lower -= 1;
    }
    if (minLat % 1 == 0 && minLat > MIN_LAT) {
      LOG.debug("Min Latitude lies on a boundary");
      lower -= 2 * MAX_LON;
    }

    // Clip to the cell limit
    lower = Math.max(0, lower);
    upper = Math.min(2 * MAX_LON * 2 * MAX_LAT - 1, upper);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting cells contained in " + lower + " to " + upper);
    }

    /*
    TODO: This does not work properly!
    int omitLeft = lower % 2 * MAX_LON;
    int omitRight = upper % 2 * MAX_LON;
    if (omitRight == 0) {
      omitRight = 2 * MAX_LON;
    }
    Set<Integer> cells = new HashSet<Integer>();
    for (int i = lower; i <= upper; i++) {
      if (i % 2 * MAX_LON >= omitLeft && i % 2 * MAX_LON <= omitRight) {
        cells.add(i);
      }
    }
    */

    Set<Integer> cells = new HashSet<Integer>();
    for (int i = lower; i <= upper; i++) {
      cells.add(i);

    }

    return cells;
  }

  public static void main(String... args) throws HiveException {
    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
    OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");

    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
    BytesWritable writable = GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry);
    CellUdf udf = new CellUdf();
    udf.evaluate(writable);
  }

}
