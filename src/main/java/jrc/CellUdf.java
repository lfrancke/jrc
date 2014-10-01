package jrc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

public class CellUdf extends UDF {

  private static final Log LOG = LogFactory.getLog(CellUdf.class);
  private static final int MIN_LAT = -90;
  private static final int MAX_LAT = 90;
  private static final int MIN_LON = -180;
  private static final int MAX_LON = 180;

  private static final SpatialReference SPATIAL_REFERENCE = SpatialReference.create(4326);
  private final OperatorIntersects operator =
    (OperatorIntersects) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersects);
  private final LoadingCache<Integer, Envelope> cellEnvelopes =
    CacheBuilder.newBuilder().maximumSize(100000).build(new CacheLoader<Integer, Envelope>() {
      @Override
      public Envelope load(Integer cell) {
        return initCellEnvelope(cell);
      }
    });
  private Double cellSize;
  private int maxLonCell;
  private int maxLatCell;

  public static void main(String... args) throws HiveException {
    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((-179.8 -89.8, -179.2 -89.8, -179.2 -89.2, -179.8 -89.2, -179.8 -89.8))");
    OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
    //OGCGeometry ogcGeometry = OGCGeometry.fromText("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))");
    BytesWritable writable = GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry);
    CellUdf udf = new CellUdf();
    List<Integer> evaluate = udf.evaluate(1, writable);
    for (Integer integer : evaluate) {
      System.out.println(integer);
    }
  }

  public List<Integer> evaluate(double cellSize, BytesWritable b) throws HiveException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    if (this.cellSize == null) {
      this.cellSize = cellSize;
      maxLonCell = (int) Math.floor((2 * MAX_LON) / cellSize);
      maxLatCell = (int) Math.floor((2 * MAX_LAT) / cellSize);
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
      getCellsEnclosedBy(envBound.getYMin(), envBound.getYMax(), envBound.getXMin(), envBound.getXMax(), cellSize);
    LOG.debug("Potential cells found: " + cellsEnclosedBy.size());

    // 3. Check candidate cells
    try {
      cellsEnclosedBy = getCellIntersects(ogcGeometry, cellsEnclosedBy);
    } catch (ExecutionException e) {
      throw new HiveException("Error doing cell intersects", e);
    }
    LOG.debug("Actual cells found: " + cellsEnclosedBy.size());

    return Lists.newArrayList(cellsEnclosedBy);
  }

  private Envelope initCellEnvelope(int cell) {
    int row = cell / maxLonCell;
    int col = cell % maxLonCell;
    Envelope envelope = new Envelope(MIN_LON + col * cellSize,
                                     MIN_LAT + row * cellSize,
                                     MIN_LON + col * cellSize + cellSize,
                                     MIN_LAT + row * cellSize + cellSize);
    operator.accelerateGeometry(envelope, SPATIAL_REFERENCE, Geometry.GeometryAccelerationDegree.enumHot);
    return envelope;
  }

  private int toCellId(double latitude, double longitude, double cellSize) throws HiveException {
    if (latitude < MIN_LAT || latitude > MAX_LAT || longitude < MIN_LON || longitude > MAX_LON) {
      throw new HiveException("Invalid coordinates");
    } else {
      int la = getLatitudeId(latitude, cellSize);
      int lo = getLongitudeId(longitude, cellSize);
      return Math.min(Math.max(la + lo, 0), maxLatCell * maxLonCell - 1);
    }
  }

  /*
     floor((latitude + MAX_LAT) / cellSize) * ((2 * MAX_LON) / cellSize)
   */
  private int getLatitudeId(double latitude, double cellSize) {
    return new Double(Math.floor((latitude + MAX_LAT) / cellSize) * maxLonCell).intValue();
  }

  /*
     cell: floor((longitude + MAX_LON) / cell size)
     max:  2 * MAX_LON / cell size)
   */
  private int getLongitudeId(double longitude, double cellSize) {
    return new Double(Math.floor((longitude + MAX_LON) / cellSize)).intValue();
  }

  private Set<Integer> getCellsEnclosedBy(double minLat, double maxLat, double minLon, double maxLon, double cellSize)
    throws HiveException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Establishing cells enclosed by (lon/lat), min: "
                + minLon
                + "/"
                + minLat
                + ", max: "
                + maxLon
                + "/"
                + maxLat);
    }

    // Create a 1 cell buffer around the area in question
    minLat = Math.max(MIN_LAT, minLat - cellSize);
    minLon = Math.max(MIN_LON, minLon - cellSize);

    maxLat = Math.min(MAX_LAT, maxLat + cellSize);
    maxLon = Math.min(MAX_LON, maxLon + cellSize);

    int lower = toCellId(minLat, minLon, cellSize);
    int upper = toCellId(maxLat, maxLon, cellSize);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Unprocessed cells: " + lower + " -> " + upper);
    }

    // Clip to the cell limit
    lower = Math.max(0, lower);
    upper = Math.min(maxLonCell * maxLatCell - 1, upper);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting cells contained in " + lower + " to " + upper);
    }

    int omitLeft = lower % maxLonCell;
    int omitRight = upper % maxLonCell;
    if (omitRight == 0) {
      omitRight = maxLonCell;
    }
    Set<Integer> cells = new HashSet<Integer>();
    for (int i = lower; i <= upper; i++) {
      if (i % maxLonCell >= omitLeft && i % maxLonCell <= omitRight) {
        cells.add(i);
      }
    }
    return cells;
  }

  private Set<Integer> getCellIntersects(OGCGeometry ogcGeometry, Iterable<Integer> cellsEnclosedBy)
    throws ExecutionException {
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

  private boolean intersects(int cell, OGCGeometry ogcGeometry) throws ExecutionException {
    return operator.execute(ogcGeometry.getEsriGeometry(),
                            cellEnvelopes.get(cell),
                            ogcGeometry.getEsriSpatialReference(),
                            null);
  }

}
