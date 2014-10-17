package jrc.geotools;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * UDF to calculate the Area of a given cell.
 */
public class CellAreaUdf extends UDF {

  private static final int MIN_LON = -180;
  private static final int MAX_LON = 180;
  private static final int MIN_LAT = -90;
  private final GeometryCoordinateSequenceTransformer transformer = new GeometryCoordinateSequenceTransformer();
  private long maxLonCell;
  private boolean firstRun = true;

  public static void main(String... args) throws FactoryException, TransformException {
    CellAreaUdf cellAreaUdf = new CellAreaUdf();
    NumberFormat formatter = new DecimalFormat("#0.00");
    double evaluate = cellAreaUdf.evaluate(32580, 1, "EPSG:4326", "EPSG:54009");
    System.out.println(formatter.format(evaluate / 1000000));
  }

  public double evaluate(long cellId, double cellSize, String sourceCrsString, String targetCrsString)
    throws FactoryException, TransformException {

    if (firstRun) {
      maxLonCell = (int) Math.floor((2 * MAX_LON) / cellSize);
      CoordinateReferenceSystem sourceCrs = CRS.decode(sourceCrsString);
      CoordinateReferenceSystem targetCrs = CRS.decode(targetCrsString);
      MathTransform mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
      transformer.setMathTransform(mathTransform);
      firstRun = false;
    }

    int row = (int) (cellId / maxLonCell);
    int col = (int) (cellId % maxLonCell);

    Coordinate bottomLeft = new Coordinate(MIN_LON + col * cellSize, MIN_LAT + row * cellSize);
    Coordinate bottomRight = new Coordinate(MIN_LON + (col + 1) * cellSize, MIN_LAT + row * cellSize);
    Coordinate topRight = new Coordinate(MIN_LON + (col + 1) * cellSize, MIN_LAT + (row + 1) * cellSize);
    Coordinate topLeft = new Coordinate(MIN_LON + col * cellSize, MIN_LAT + (row + 1) * cellSize);

    Polygon polygon =
      new GeometryFactory().createPolygon(new Coordinate[] {bottomLeft, bottomRight, topRight, topLeft, bottomLeft});

    return transformer.transform(polygon).getArea();
  }

}
