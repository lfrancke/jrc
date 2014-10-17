package jrc.geotools;

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import jrc.CellCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import static jrc.CellCalculator.MAX_LAT;
import static jrc.CellCalculator.MAX_LON;
import static jrc.CellCalculator.MIN_LAT;
import static jrc.CellCalculator.MIN_LON;

public class CellUdtf extends GenericUDTF {

  private static final Log LOG = LogFactory.getLog(CellUdtf.class);
  private final WKBReader reader = new WKBReader();
  private final Object[] result = new Object[2];
  private final LongWritable cellWritable = new LongWritable();
  private final BooleanWritable fullyCoveredWritable = new BooleanWritable();
  private final CellCalculator<Polygon> cellCalculator = new GeotoolsCellCalculator();
  private DoubleObjectInspector doi;
  private BinaryObjectInspector boi;
  private boolean firstRun = true;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != 2) {
      throw new UDFArgumentLengthException("cell() takes two arguments: cell size and geometry");
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

    doi = (DoubleObjectInspector) argOIs[0];
    boi = (BinaryObjectInspector) argOIs[1];

    fieldNames.add("cell");
    fieldNames.add("fully_covered");
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
    result[0] = cellWritable;
    result[1] = fullyCoveredWritable;

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    if (firstRun) {
      cellCalculator.setCellSize(doi.get(args[0]));
      firstRun = false;
    }

    Geometry geom;
    try {
      geom = reader.read(boi.getPrimitiveWritableObject(args[1]).getBytes());
    } catch (ParseException e) {
      throw new HiveException(e);
    }
    if (geom == null) {
      LOG.warn("Geometry is null");
      return;
    }

    if (geom.isEmpty()) {
      LOG.warn("Geometry is empty");
      return;
    }

    Envelope envBound = geom.getEnvelopeInternal();

    getCellsEnclosedBy(envBound.getMinY(), envBound.getMaxY(), envBound.getMinX(), envBound.getMaxX(), geom);
  }

  @Override
  public void close() throws HiveException {
  }

  private void getCellsEnclosedBy(double minLat, double maxLat, double minLon, double maxLon, Geometry geometry)
    throws HiveException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Establishing cells enclosed by (lon/lat), min: "
                + minLon
                + "/"
                + minLat
                + ", max: "
                + maxLon
                + "/"
                + maxLat);
    }

    // Create a 1 cell buffer around the area in question
    minLat = Math.max(MIN_LAT, minLat - cellCalculator.getCellSize());
    minLon = Math.max(MIN_LON, minLon - cellCalculator.getCellSize());

    maxLat = Math.min(MAX_LAT, maxLat + cellCalculator.getCellSize());
    maxLon = Math.min(MAX_LON, maxLon + cellCalculator.getCellSize());

    long lower = cellCalculator.toCellId(minLat, minLon);
    long upper = cellCalculator.toCellId(maxLat, maxLon);

    // Clip to the cell limit
    lower = Math.max(0, lower);
    upper = Math.min(cellCalculator.getMaxLonCell() * cellCalculator.getMaxLatCell() - 1, upper);

    LOG.info("Checking cells between " + lower + " and " + upper);

    long omitLeft = lower % cellCalculator.getMaxLonCell();
    long omitRight = upper % cellCalculator.getMaxLonCell();
    if (omitRight == 0) {
      omitRight = cellCalculator.getMaxLonCell();
    }

    for (long i = lower; i <= upper; i++) {
      if (i % cellCalculator.getMaxLonCell() >= omitLeft && i % cellCalculator.getMaxLonCell() <= omitRight) {
        Polygon cell = cellCalculator.getCellEnvelope(i);
        if (cell.intersects(geometry)) {
          if (cell.contains(geometry)) {
            cellWritable.set(i);
            fullyCoveredWritable.set(true);
            forward(result);
          } else {
            cellWritable.set(i);
            fullyCoveredWritable.set(false);
            forward(result);
          }
        }
      }

    }
  }

}
