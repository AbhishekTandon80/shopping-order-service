package shopping.cart.repository;

import akka.projection.r2dbc.javadsl.R2dbcSession; 
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import shopping.cart.ItemPopularity;

public interface ItemPopularityRepository {

  CompletionStage<Long> saveOrUpdate(R2dbcSession session, ItemPopularity itemPopularity);

  CompletionStage<Optional<ItemPopularity>> findById(R2dbcSession session, String id);

  CompletionStage<Optional<Long>> getCount(R2dbcSession session, String id);
}
