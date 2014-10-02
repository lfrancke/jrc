package jrc;

import java.util.ArrayList;
import java.util.List;

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
  private final LoadingCache<Long, Envelope> cellEnvelopes =
    CacheBuilder.newBuilder().maximumSize(100000).build(new CacheLoader<Long, Envelope>() {
      @Override
      public Envelope load(Long cell) {
        return initCellEnvelope(cell);
      }
    });
  private Double cellSize;
  private long maxLonCell;
  private long maxLatCell;

  public static void main(String... args) throws HiveException {
    OGCGeometry ogcGeometry =
      //OGCGeometry.fromText("POLYGON ((-179.8 -89.8, -179.2 -89.8, -179.2 -89.2, -179.8 -89.2, -179.8 -89.8))");
      //OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
      //OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
      OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
    //OGCGeometry.fromText("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))");
    BytesWritable writable = GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry);
    CellUdf udf = new CellUdf();
    List<Long> evaluate = udf.evaluate(0.01, writable);
    for (Long integer : evaluate) {
      //System.out.println(integer);
    }
  }

  public List<Long> evaluate(double cellSize, BytesWritable b) throws HiveException {
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
    List<Long> cellsEnclosedBy = getCellsEnclosedBy(envBound.getYMin(),
                                                    envBound.getYMax(),
                                                    envBound.getXMin(),
                                                    envBound.getXMax(),
                                                    cellSize,
                                                    ogcGeometry);
    LOG.info("Cells found: " + cellsEnclosedBy.size());
    return cellsEnclosedBy;
  }

  private Envelope initCellEnvelope(long cell) {
    long row = cell / maxLonCell;
    long col = cell % maxLonCell;
    Envelope envelope = new Envelope(MIN_LON + col * cellSize,
                                     MIN_LAT + row * cellSize,
                                     MIN_LON + col * cellSize + cellSize,
                                     MIN_LAT + row * cellSize + cellSize);
    operator.accelerateGeometry(envelope, SPATIAL_REFERENCE, Geometry.GeometryAccelerationDegree.enumHot);
    return envelope;
  }

  private long toCellId(double latitude, double longitude, double cellSize) throws HiveException {
    if (latitude < MIN_LAT || latitude > MAX_LAT || longitude < MIN_LON || longitude > MAX_LON) {
      throw new HiveException("Invalid coordinates");
    } else {
      long la = getLatitudeId(latitude, cellSize);
      long lo = getLongitudeId(longitude, cellSize);
      return Math.min(Math.max(la + lo, 0), maxLatCell * maxLonCell - 1);
    }
  }

  /*
     floor((latitude + MAX_LAT) / cellSize) * ((2 * MAX_LON) / cellSize)
   */
  private long getLatitudeId(double latitude, double cellSize) {
    return new Double(Math.floor((latitude + MAX_LAT) / cellSize) * maxLonCell).longValue();
  }

  /*
     cell: floor((longitude + MAX_LON) / cell size)
     max:  2 * MAX_LON / cell size)
   */
  private long getLongitudeId(double longitude, double cellSize) {
    return new Double(Math.floor((longitude + MAX_LON) / cellSize)).longValue();
  }

  private List<Long> getCellsEnclosedBy(
    double minLat, double maxLat, double minLon, double maxLon, double cellSize, OGCGeometry ogcGeometry
  ) throws HiveException {

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

    long lower = toCellId(minLat, minLon, cellSize);
    long upper = toCellId(maxLat, maxLon, cellSize);

    LOG.info("Unprocessed cells: " + lower + " -> " + upper);

    // Clip to the cell limit
    lower = Math.max(0, lower);
    upper = Math.min(maxLonCell * maxLatCell - 1, upper);

    LOG.info("Checking cells between " + lower + " and " + upper);

    long omitLeft = lower % maxLonCell;
    long omitRight = upper % maxLonCell;
    if (omitRight == 0) {
      omitRight = maxLonCell;
    }

    operator.accelerateGeometry(ogcGeometry.getEsriGeometry(),
                                ogcGeometry.getEsriSpatialReference(),
                                Geometry.GeometryAccelerationDegree.enumHot);

    List<Long> cellsEnclosedBy = new ArrayList<Long>(10000);

    for (long i = lower; i <= upper; i++) {
      if (i % maxLonCell >= omitLeft && i % maxLonCell <= omitRight) {
        if (intersects(i, ogcGeometry)) {
          cellsEnclosedBy.add(i);
        }
      }

    }
    return cellsEnclosedBy;

  }

  private boolean intersects(long cell, OGCGeometry ogcGeometry) {
    return operator.execute(ogcGeometry.getEsriGeometry(),
                            initCellEnvelope(cell),
                            ogcGeometry.getEsriSpatialReference(),
                            null);
  }

}
