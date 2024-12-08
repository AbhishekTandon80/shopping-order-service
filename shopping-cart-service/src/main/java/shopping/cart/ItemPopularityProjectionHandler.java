
package shopping.cart;

import akka.Done;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.r2dbc.javadsl.R2dbcHandler;
import akka.projection.r2dbc.javadsl.R2dbcSession;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.repository.ItemPopularityRepository;

public final class ItemPopularityProjectionHandler
    extends R2dbcHandler<EventEnvelope<ShoppingCart.Event>> { 
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String slice;
  private final ItemPopularityRepository repo;

  public ItemPopularityProjectionHandler(String slice, ItemPopularityRepository repo) {
    this.slice = slice;
    this.repo = repo;
  }

  private CompletionStage<ItemPopularity> findOrNew(R2dbcSession session, String itemId) {
    return repo.findById(session, itemId)
        .thenApply(
            itemPopularity -> itemPopularity.orElseGet(() -> new ItemPopularity(itemId, 0L)));
  }

  @Override
  public CompletionStage<Done> process(
      R2dbcSession session, EventEnvelope<ShoppingCart.Event> envelope) { 
    ShoppingCart.Event event = envelope.event();

    switch (event) {
      case ShoppingCart.ItemAdded(var __, String itemId, int qtd) -> { 
        var itemPopularity = new ItemPopularity(itemId, qtd);
        var updated = repo.saveOrUpdate(session, itemPopularity);
        return updated.thenApply(
            rows -> {
              logCount(itemId, rows);
              return Done.getInstance();
            });
        
      }
      case ShoppingCart.ItemQuantityAdjusted(var __, String itemId, int oldQtd, int newQtd) -> {
        return findOrNew(session, itemId)
            .thenApply(itemPop -> itemPop.changeCount(newQtd - oldQtd))
            .thenCompose(itm -> repo.saveOrUpdate(session, itm))
            .thenApply(
                rows -> {
                  logCount(itemId, rows);
                  return Done.getInstance();
                });
      }
      case ShoppingCart.ItemRemoved(var __, String itemId, int oldQtd) -> {
        return findOrNew(session, itemId)
            .thenApply(itemPop -> itemPop.changeCount(-oldQtd))
            .thenCompose(itm -> repo.saveOrUpdate(session, itm))
            .thenApply(
                rows -> {
                  logCount(itemId, rows);
                  return Done.getInstance();
                });
        
      }
      case ShoppingCart.CheckedOut ignored -> {
        return CompletableFuture.completedFuture(Done.getInstance());
      }
      case null, default -> {
        throw new IllegalArgumentException("Unknown event type: " + event.getClass());
      }
    }
  }

  private CompletionStage<Done> getCount(R2dbcSession session, String itemId) {
    return repo.getCount(session, itemId)
        .thenApply(
            optLong -> {
              optLong.ifPresent(aLong -> logCount(itemId, aLong));
              return Done.getInstance();
            });
  }

  private void logCount(String itemId, Long count) {
    logger.info(
        "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
        this.slice,
        itemId,
        count);
  }
}

