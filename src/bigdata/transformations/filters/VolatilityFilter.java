package bigdata.transformations.filters;


import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetFeatures;
import scala.Tuple2;

public class VolatilityFilter implements Function<Tuple2<String, AssetFeatures>, Boolean>{
	
	private static final long serialVersionUID = 1L;
	private final double volatilityCeiling;
	
	public VolatilityFilter(double volatilityCeiling) {
		this.volatilityCeiling = volatilityCeiling;
	}

	

	@Override
	public Boolean call(Tuple2<String, AssetFeatures> pair) throws Exception {
		// TODO Auto-generated method stub
		return pair._2().getAssetVolitility() < volatilityCeiling;
	}

}
