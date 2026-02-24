package bigdata.transformations.filters;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import scala.Tuple2;

public class PERatioFilter implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean>{
	private static final long seriaVersionUID = 1L;
	
	private final double peRatioThreshold;
	
	public PERatioFilter(double peRatioThreshold) {
		this.peRatioThreshold = peRatioThreshold;
	}

	@Override
	public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> pair) throws Exception {
		// TODO Auto-generated method stub
		double peRatio = pair._2()._2().getPriceEarningRatio();
		return peRatio < peRatioThreshold;
	}

}
