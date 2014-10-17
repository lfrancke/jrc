package jrc.geotools;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

public class GeotoolsIntersectsUdf extends UDF {

  private final WKBReader reader = new WKBReader();

  public boolean evaluate(BytesWritable a, BytesWritable b) throws ParseException {
    Geometry geom1 = reader.read(a.getBytes());
    Geometry geom2 = reader.read(b.getBytes());

    return geom1.intersects(geom2);
  }

}
