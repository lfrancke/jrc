package jrc;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class TransformUdf extends UDF {

  private static final Log LOG = LogFactory.getLog(CellUdf.class);

  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();
  private GeometryCoordinateSequenceTransformer transformer;
  private final BytesWritable transformedWritable = new BytesWritable();

  public BytesWritable evaluate(String sourceCrsString, String targetCrsString, BytesWritable b)
    throws HiveException, ParseException, FactoryException, TransformException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    Geometry geometry = reader.read(b.getBytes());

    if (geometry == null) {
      LOG.warn("Geometry is null");
      return null;
    }

    if (transformer == null) {
      CoordinateReferenceSystem sourceCrs = CRS.decode(sourceCrsString);
      CoordinateReferenceSystem targetCrs = CRS.decode(targetCrsString);
      MathTransform mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
      transformer = new GeometryCoordinateSequenceTransformer();
      transformer.setMathTransform(mathTransform);
    }

    Geometry transformed = transformer.transform(geometry);
    byte[] transformedBytes = writer.write(transformed);
    transformedWritable.set(transformedBytes, 0, transformedBytes.length);
    return transformedWritable;
  }

}
