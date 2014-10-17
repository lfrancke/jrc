package jrc.esri;

import java.nio.ByteBuffer;

import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;

public class EsriAreaUdf extends UDF {

  private final DoubleWritable result = new DoubleWritable();

  public DoubleWritable evaluate(BytesWritable a) {
    OGCGeometry ogcGeometry = OGCGeometry.fromBinary(ByteBuffer.wrap(a.getBytes()));
    result.set(ogcGeometry.getEsriGeometry().calculateArea2D());
    return result;
  }

}
