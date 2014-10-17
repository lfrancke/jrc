package jrc.geotools;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import jrc.CellCalculator;

public class GeotoolsCellCalculator extends CellCalculator<Polygon> {

  private final GeometryFactory factory = new GeometryFactory();

  @Override
  public Polygon getCellEnvelope(long cell) {
    long row = cell / getMaxLonCell();
    long col = cell % getMaxLonCell();

    double xMin = MIN_LON + col * getCellSize();
    double yMin = MIN_LAT + row * getCellSize();
    double xMax = MIN_LON + col * getCellSize() + getCellSize();
    double yMax = MIN_LAT + row * getCellSize() + getCellSize();

    return factory.createPolygon(
      factory.createLinearRing(
        new Coordinate[] {
          new Coordinate(xMin, yMin),
          new Coordinate(xMax, yMin),
          new Coordinate(xMax, yMax),
          new Coordinate(xMin, yMax),
          new Coordinate(xMin, yMin)
        }
      ), null);

  }

}
