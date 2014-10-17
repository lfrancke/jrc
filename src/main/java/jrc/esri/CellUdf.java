package jrc.esri;

import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

/**
 * A UDF that returns an array of cells for a given Geometry and cell size.
 */
@Deprecated
public class CellUdf extends BaseCellUdf {

  private static final Log LOG = LogFactory.getLog(CellUdf.class);

  public static void main(String... args) throws HiveException {
    OGCGeometry ogcGeometry =
      //OGCGeometry.fromText("POLYGON ((-179.8 -89.8, -179.2 -89.8, -179.2 -89.2, -179.8 -89.2, -179.8 -89.8))");
      //OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
      //OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
      OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
    //OGCGeometry.fromText("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))");
    BytesWritable writable = GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry);
    CellUdf udf = new CellUdf();
    List<Long> evaluate = udf.evaluate(0.05, writable);
    for (Long integer : evaluate) {
      //System.out.println(integer);
    }
  }

  public List<Long> evaluate(double cellSize, BytesWritable b) throws HiveException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    setCellSize(cellSize);

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

    // TODO: from geotrellis
    // If the point is inside the area of the cell, it is included in the cell.
    // If the point lies on the north or west border of the cell, it is included in the cell.
    // If the point lies on the south or east border of the cell, it is not included in the cell, it is included in the next southern or eastern cell, respectively.

    // Create a 1 cell buffer around the area in question
    minLat = Math.max(MIN_LAT, minLat - cellSize);
    minLon = Math.max(MIN_LON, minLon - cellSize);

    maxLat = Math.min(MAX_LAT, maxLat + cellSize);
    maxLon = Math.min(MAX_LON, maxLon + cellSize);

    long lower = toCellId(minLat, minLon, cellSize);
    long upper = toCellId(maxLat, maxLon, cellSize);

    // Clip to the cell limit
    lower = Math.max(0, lower);
    upper = Math.min(getMaxLonCell() * getMaxLatCell() - 1, upper);

    LOG.info("Checking cells between " + lower + " and " + upper);

    long omitLeft = lower % getMaxLonCell();
    long omitRight = upper % getMaxLonCell();
    if (omitRight == 0) {
      omitRight = getMaxLonCell();
    }

    getIntersectsOperator().accelerateGeometry(ogcGeometry.getEsriGeometry(),
                                               ogcGeometry.getEsriSpatialReference(),
                                               Geometry.GeometryAccelerationDegree.enumHot);

    List<Long> cellsEnclosedBy = new ArrayList<Long>(10000);

    for (long i = lower; i <= upper; i++) {
      if (i % getMaxLonCell() >= omitLeft && i % getMaxLonCell() <= omitRight) {
        if (intersects(i, ogcGeometry)) {
          cellsEnclosedBy.add(i);
        }
      }

    }
    return cellsEnclosedBy;

  }

  private boolean intersects(long cell, OGCGeometry ogcGeometry) {
    return getIntersectsOperator().execute(ogcGeometry.getEsriGeometry(),
                                           getCellEnvelope(cell),
                                           ogcGeometry.getEsriSpatialReference(),
                                           null);
  }

}
