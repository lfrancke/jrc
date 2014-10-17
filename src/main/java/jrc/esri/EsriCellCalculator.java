package jrc.esri;

import com.esri.core.geometry.Envelope;
import jrc.CellCalculator;

public class EsriCellCalculator extends CellCalculator<Envelope> {

  private final Envelope envelope = new Envelope();

  @Override
  public Envelope getCellEnvelope(long cell) {
    long row = cell / getMaxLonCell();
    long col = cell % getMaxLonCell();
    envelope.setCoords(MIN_LON + col * getCellSize(),
                       MIN_LAT + row * getCellSize(),
                       MIN_LON + col * getCellSize() + getCellSize(),
                       MIN_LAT + row * getCellSize() + getCellSize());
    //intersectsOperator.accelerateGeometry(envelope, SPATIAL_REFERENCE, Geometry.GeometryAccelerationDegree.enumHot);
    return envelope;

  }
}
