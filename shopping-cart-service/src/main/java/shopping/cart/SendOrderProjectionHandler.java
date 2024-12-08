package shopping.cart;

import static akka.Done.done;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.r2dbc.javadsl.R2dbcHandler;
import akka.projection.r2dbc.javadsl.R2dbcSession;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.order.proto.Item;
import shopping.order.proto.OrderRequest;
import shopping.order.proto.ShoppingOrderService;

public final class SendOrderProjectionHandler
    extends R2dbcHandler<EventEnvelope<ShoppingCart.Event>> {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final ClusterSharding sharding;
  private final Duration timeout;
  private final ShoppingOrderService orderService;

  private final String slice;

  public SendOrderProjectionHandler(
      ActorSystem<?> system, ShoppingOrderService orderService, String slice) { 
    this.slice = slice;
    sharding = ClusterSharding.get(system);
    timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout");
    this.orderService = orderService;
  }

  @Override
  public CompletionStage<Done> process(
      R2dbcSession session, EventEnvelope<ShoppingCart.Event> envelope) {
    return envelope.event() instanceof ShoppingCart.CheckedOut checkedOut
        ? sendOrder(checkedOut)
        : CompletableFuture.completedFuture(done());
  }

  private CompletionStage<Done> sendOrder(ShoppingCart.CheckedOut checkout) {
    var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, checkout.cartId());
    var reply = entityRef.ask(ShoppingCart.Get::new, timeout);

    return reply.thenCompose(
        cart -> { 
          List<Item> protoItems =
              cart.items().entrySet().stream()
                  .map(
                      entry ->
                          Item.newBuilder()
                              .setItemId(entry.getKey())
                              .setQuantity(entry.getValue())
                              .build())
                  .collect(Collectors.toList());
          log.info(
              "Sending order of {} items for cart {}.", cart.items().size(), checkout.cartId());
          var orderRequest =
              OrderRequest.newBuilder()
                  .setCartId(checkout.cartId())
                  .addAllItems(protoItems)
                  .build();
          return orderService.order(orderRequest).thenApply(response -> done()); 
        });
  }
}
