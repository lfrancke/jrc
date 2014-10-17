package jrc.esri;

import java.nio.ByteBuffer;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersection;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * Returns the intersection geometry between a cell and a given geometry.
 * Works on WKB.
 */
public class CellIntersectsUdf extends BaseCellUdf {

  private static final Log LOG = LogFactory.getLog(CellIntersectsUdf.class);

  private final OperatorIntersection intersectionOperator =
    (OperatorIntersection) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersection);

  private final BytesWritable result = new BytesWritable();

  public BytesWritable evaluate(double cellSize, long cell, BytesWritable b) {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    setCellSize(cellSize);

    OGCGeometry ogcGeometry = OGCGeometry.fromBinary(ByteBuffer.wrap(b.getBytes()));
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
    ByteBuffer buffer = esriGeometry.asBinary();
    result.set(buffer.array(), buffer.arrayOffset(), buffer.position());
    return result;
  }

}
