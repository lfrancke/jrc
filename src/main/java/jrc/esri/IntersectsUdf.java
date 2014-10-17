package jrc.esri;

import java.nio.ByteBuffer;

import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Calculates whether two geometries intersect.
 * Expects WKB.
 */
public class IntersectsUdf extends UDF {

  private final OperatorIntersects intersectsOperator =
    (OperatorIntersects) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersects);

  public boolean evaluate(BytesWritable a, BytesWritable b) {
    OGCGeometry geom1 = OGCGeometry.fromBinary(ByteBuffer.wrap(a.getBytes()));
    OGCGeometry geom2 = OGCGeometry.fromBinary(ByteBuffer.wrap(a.getBytes()));

    if (geom1 == null || geom2 == null) {
      return false;
    }

    return intersectsOperator.execute(geom1.getEsriGeometry(),
                                      geom2.getEsriGeometry(),
                                      geom1.getEsriSpatialReference(),
                                      null);

  }

}
