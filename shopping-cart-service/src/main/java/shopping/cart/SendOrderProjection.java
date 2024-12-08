package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.japi.Pair;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.ExactlyOnceProjection;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.R2dbcProjectionSettings;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import java.util.List;
import java.util.Optional;
import shopping.order.proto.ShoppingOrderService;

public class SendOrderProjection {

  private SendOrderProjection() {}

  public static void init(ActorSystem<?> system, ShoppingOrderService orderService) {
    ShardedDaemonProcess.get(system)
        .initWithContext(
            ProjectionBehavior.Command.class,
            "SendOrderProjection",
            4,
            daemonContext -> {
              List<Pair<Integer, Integer>> sliceRanges =
                  EventSourcedProvider.sliceRanges(
                      system, R2dbcReadJournal.Identifier(), daemonContext.totalProcesses());
              Pair<Integer, Integer> sliceRange = sliceRanges.get(daemonContext.processNumber());
              return ProjectionBehavior.create(createProjection(system, orderService, sliceRange));
            },
            ShardedDaemonProcessSettings.create(system),
            Optional.of(ProjectionBehavior.stopMessage()));
  }

  private static ExactlyOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> createProjection(
      ActorSystem<?> system,
      ShoppingOrderService shoppingOrderService,
      Pair<Integer, Integer> sliceRange) {

    int minSlice = sliceRange.first();
    int maxSlice = sliceRange.second();

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsBySlices(
            system, R2dbcReadJournal.Identifier(), "ShoppingCart", minSlice, maxSlice);

    String slice = "carts-" + minSlice + "-" + maxSlice;
    Optional<R2dbcProjectionSettings> settings = Optional.empty();
    return R2dbcProjection.exactlyOnce(
        ProjectionId.of("SendOrderProjection", slice),
        settings,
        sourceProvider,
        () -> new SendOrderProjectionHandler(system, shoppingOrderService, slice),
        system);
  }
}
