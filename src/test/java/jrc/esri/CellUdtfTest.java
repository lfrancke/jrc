package jrc.esri;

import java.io.IOException;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.json.JSONException;

public class CellUdtfTest {

    public static void main(String... args) throws HiveException, IOException, JSONException {
    OGCGeometry ogcGeometry =
      //OGCGeometry.fromText("POLYGON ((-179.8 -89.8, -179.2 -89.8, -179.2 -89.2, -179.8 -89.2, -179.8 -89.8))");
      //OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
      //OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
      //OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
    //OGCGeometry.fromText("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))");


    OGCGeometry.fromGeoJson(Resources.toString(Resources.getResource("single_geometry_json.json"), Charsets.US_ASCII));
    BytesWritable writable = GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry);
    CellUdtf udf = new CellUdtf();

    ObjectInspector[] oi = {
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    };
    udf.initialize(oi);


    Object[] udfArgs = {
      0.1,
      writable
    };
    udf.process(udfArgs);

    //List<Long> evaluate = udf.evaluate(0.01, writable);
  }


}
