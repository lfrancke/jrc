package jrc;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

public class CellIntersectsUdf extends BaseCellUdf {

  private static final Log LOG = LogFactory.getLog(CellIntersectsUdf.class);

  private final OperatorIntersection intersectionOperator =
    (OperatorIntersection) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersection);

  public BytesWritable evaluate(double cellSize, long cell, BytesWritable b) {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    setCellSize(cellSize);

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

    Envelope cellEnvelope = getCellEnvelope(cell);
    Geometry geometry =
      intersectionOperator.execute(cellEnvelope, ogcGeometry.getEsriGeometry(), SPATIAL_REFERENCE, null);

    OGCGeometry esriGeometry = OGCGeometry.createFromEsriGeometry(geometry, SPATIAL_REFERENCE);
    return GeometryUtils.geometryToEsriShapeBytesWritable(esriGeometry);
  }

}
