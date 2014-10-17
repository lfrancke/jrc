package jrc.geotools;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;

public class GeotoolsAreaUdf extends UDF {

  private final DoubleWritable result = new DoubleWritable();
  private final WKBReader reader = new WKBReader();

  public DoubleWritable evaluate(BytesWritable a) throws ParseException {
    Geometry geometry = reader.read(a.getBytes());
    result.set(geometry.getArea());
    return result;
  }

}
