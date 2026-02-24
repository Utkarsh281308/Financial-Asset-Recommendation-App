package bigdata.transformations.maps;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.StockPrice;
import scala.Tuple2;

public class StockPriceToPairMap implements PairFunction<StockPrice, String, StockPrice> {
	private static final long serialversionUID = 1L;

	@Override
	public Tuple2<String, StockPrice> call(StockPrice price) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<String, StockPrice>(price.getStockTicker(), price);
		
	}
	

}
