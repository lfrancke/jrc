package jrc.geotools;

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

/**
 * Transforms a geometry from one CRS into another.
 * Expects WKB, returns WKB.
 */
public class TransformUdf extends UDF {

  private static final Log LOG = LogFactory.getLog(TransformUdf.class);

  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();
  private final GeometryCoordinateSequenceTransformer transformer = new GeometryCoordinateSequenceTransformer();
  private final BytesWritable transformedWritable = new BytesWritable();
  private boolean firstRun = true;

  public BytesWritable evaluate(String sourceCrsString, String targetCrsString, BytesWritable b)
    throws HiveException, ParseException, FactoryException, TransformException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    Geometry geometry = reader.read(b.getBytes());

    if (firstRun) {
      CoordinateReferenceSystem sourceCrs = CRS.decode(sourceCrsString);
      CoordinateReferenceSystem targetCrs = CRS.decode(targetCrsString);
      MathTransform mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
      transformer.setMathTransform(mathTransform);
      firstRun = false;
    }

    Geometry transformed = transformer.transform(geometry);
    byte[] transformedBytes = writer.write(transformed);
    transformedWritable.set(transformedBytes, 0, transformedBytes.length);
    return transformedWritable;
  }

}
