package jrc.esri;

import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import jrc.BaseCellUdtf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class CellIntersectsUdtf extends BaseCellUdtf {

  private final OperatorIntersection intersectionOperator =
    (OperatorIntersection) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersection);
  private DoubleObjectInspector doi;
  private BinaryObjectInspector boi;
  private ListObjectInspector loi;
  private LongObjectInspector eoi;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != 3) {
      throw new UDFArgumentLengthException(
        "cell_intersects() takes three arguments: cell size, geometry, array of cells");
    }

    List<String> fieldNames = new ArrayList<String>();
    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

    if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE || !argOIs[0].getTypeName()
      .equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      throw new UDFArgumentException("cell(): cell_size has to be a double");
    }

    if (argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE || !argOIs[1].getTypeName()
      .equals(serdeConstants.BINARY_TYPE_NAME)) {
      throw new UDFArgumentException("cell(): geom has to be binary");
    }

    if (argOIs[2].getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("cell_intersects(): cells has to be an array");
    }

    if (loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE
        || !loi.getListElementObjectInspector().getTypeName().equals(serdeConstants.BIGINT_TYPE_NAME)) {
      throw new UDFArgumentException("cell_intersects(): cells has to be an array of longs");
    }

    doi = (DoubleObjectInspector) argOIs[0];
    boi = (BinaryObjectInspector) argOIs[1];
    loi = (ListObjectInspector) argOIs[2];
    eoi = (LongObjectInspector) loi.getListElementObjectInspector();

    fieldNames.add("intersect");
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    setCellSize(doi.get(args[0]));
    OGCGeometry geom = GeometryUtils.geometryFromEsriShape(boi.getPrimitiveWritableObject(args[1]));

    for (int i = 0; i < loi.getListLength(args[2]); i++) {
      long cell = eoi.get(loi.getListElement(args[2], i));
      Envelope cellEnvelope = getCellEnvelope(cell);
      Geometry geometry = intersectionOperator.execute(cellEnvelope, geom.getEsriGeometry(), SPATIAL_REFERENCE, null);

      OGCGeometry esriGeometry = OGCGeometry.createFromEsriGeometry(geometry, SPATIAL_REFERENCE);
      forward(GeometryUtils.geometryToEsriShapeBytesWritable(esriGeometry));
    }
  }
}
