package bigdata.transformations.filters;

import org.apache.spark.api.java.function.Function;
import bigdata.objects.StockPrice;

public class DateRangeFilter implements Function<StockPrice, Boolean>{
	private static final long serialVersionUID = 1L;
	
	private final int endYear;
	private final int endMonth;
	private final int endDay;
	

	
	public DateRangeFilter(String endDate) {
		String[] parts = endDate.split("-");
		this.endYear = Integer.parseInt(parts[0]);
		this.endMonth = Integer.parseInt(parts[1]);
		this.endDay = Integer.parseInt(parts[2]);
		
		
	}

	@Override
	public Boolean call(StockPrice price) throws Exception {
		// TODO Auto-generated method stub
		int priceDate = price.getYear()* 10000 + price.getMonth() * 100 + price.getDay();
		int cutoffDate = endYear * 10000 + endMonth * 100 + endDay;
  		return priceDate <= cutoffDate;
	}

}
