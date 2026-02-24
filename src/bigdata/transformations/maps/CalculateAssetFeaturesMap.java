package bigdata.transformations.maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetFeatures;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;

public class CalculateAssetFeaturesMap implements Function<Iterable<StockPrice>, AssetFeatures>{
	private static final long serialVersionUID = 1L;
	
	private static final int RETURNS_NUM_DAYS = 5;

	@Override
	public AssetFeatures call(Iterable<StockPrice> priceIterable) throws Exception {
		// TODO Auto-generated method stub
		List<StockPrice> priceList = new ArrayList<StockPrice>();
		for(StockPrice price : priceIterable) {
			priceList.add(price);
		}
		Collections.sort(priceList, new Comparator<StockPrice>() {

			@Override
			public int compare(StockPrice a, StockPrice b) {
				// TODO Auto-generated method stub
				int dataA = a.getYear() * 10000 + a.getMonth()* 100 + a.getDay();
				int dataB = b.getYear() * 10000 + b.getMonth()* 100 + b.getDay();
				
				return Integer.compare(dataA, dataB);
			}
			
		});
		
		List<Double> closePrices = new ArrayList<Double>(priceList.size());
		for (StockPrice price : priceList) {
			closePrices.add(price.getAdjustedClosePrice());
		}
		double assetReturn = Returns.calculate(RETURNS_NUM_DAYS, closePrices);
		double assetVolatility = Volitility.calculate(closePrices);
		
		AssetFeatures features = new AssetFeatures();
		features.setAssetReturn(assetReturn);
		features.setAssetVolitility(assetVolatility);
		
		return features;
	}

}
