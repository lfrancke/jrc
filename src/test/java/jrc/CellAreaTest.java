package jrc;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import jrc.geotools.CellAreaUdf;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class CellAreaTest {

  public static void main(String... args) throws FactoryException, TransformException {
    CellAreaUdf cellAreaUdf = new CellAreaUdf();
    NumberFormat formatter = new DecimalFormat("#0.00");
    double evaluate = cellAreaUdf.evaluate(32580, 1, "EPSG:4326", "EPSG:54009");
    System.out.println(formatter.format(evaluate / 1000000));
  }

}
