package shopping.cart.repository;

import akka.projection.r2dbc.javadsl.R2dbcSession;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.ItemPopularity;

public class ItemPopularityRepositoryImpl implements ItemPopularityRepository {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public CompletionStage<Long> saveOrUpdate(R2dbcSession session, ItemPopularity itemPopularity) {
    return session.updateOne(
        session
            .createStatement(
                "INSERT INTO item_popularity (itemid, count) VALUES ($1, $2) "
                    + "ON CONFLICT (itemid) DO UPDATE SET count = item_popularity.count + $3")
            .bind(0, itemPopularity.itemId())
            .bind(1, itemPopularity.count())
            .bind(2, itemPopularity.count()));
  }

  @Override
  public CompletionStage<Optional<ItemPopularity>> findById(R2dbcSession session, String id) {
    return session.selectOne(
        session
            .createStatement("SELECT itemid, count FROM item_popularity WHERE itemid = $1")
            .bind(0, id),
        row -> new ItemPopularity(row.get("itemid", String.class), row.get("count", Long.class)));
  }

  @Override
  public CompletionStage<Optional<Long>> getCount(R2dbcSession session, String id) {
    return session.selectOne(
        session.createStatement("SELECT count FROM item_popularity WHERE itemid = $1").bind(0, id),
        row -> row.get("count", Long.class));
  }
}
