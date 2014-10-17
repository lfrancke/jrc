package jrc.geotools;

import com.vividsolutions.jts.geom.Polygon;
import jrc.CellCalculator;
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

  private final GeometryCoordinateSequenceTransformer transformer = new GeometryCoordinateSequenceTransformer();
  private final CellCalculator<Polygon> cellCalculator = new GeotoolsCellCalculator();
  private boolean firstRun = true;

  public double evaluate(long cellId, double cellSize, String sourceCrsString, String targetCrsString)
    throws FactoryException, TransformException {

    if (firstRun) {
      cellCalculator.setCellSize(cellSize);
      CoordinateReferenceSystem sourceCrs = CRS.decode(sourceCrsString);
      CoordinateReferenceSystem targetCrs = CRS.decode(targetCrsString);
      MathTransform mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
      transformer.setMathTransform(mathTransform);
      firstRun = false;
    }

    Polygon polygon = cellCalculator.getCellEnvelope(cellId);
    return transformer.transform(polygon).getArea();
  }

}
