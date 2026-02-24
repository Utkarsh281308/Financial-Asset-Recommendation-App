package bigdata.transformations.maps;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import scala.Tuple2;

public class AssetBuilderMap implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Asset>{
	private static final long serialVersionUID = 1L;

	@Override
	public Asset call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> pair) throws Exception {
		// TODO Auto-generated method stub
		String ticker = pair._1();
		AssetFeatures features = pair._2()._1();
		AssetMetadata metadata = pair._2()._2();
		
		features.setPeRatio(metadata.getPriceEarningRatio());
		
		return new Asset(
				ticker,
				features,
				metadata.getName(),
				metadata.getIndustry(),
				metadata.getSector()
				);
	}

}
