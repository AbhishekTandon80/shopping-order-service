
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
import shopping.cart.repository.ItemPopularityRepository;

public final class ItemPopularityProjection {

  private ItemPopularityProjection() {}

  
  public static void init(ActorSystem<?> system, ItemPopularityRepository repository) {

    ShardedDaemonProcess.get(system)
        .initWithContext( 
            ProjectionBehavior.Command.class,
            "ItemPopularityProjection",
            4,
            daemonContext -> {
              List<Pair<Integer, Integer>> sliceRanges = 
                  EventSourcedProvider.sliceRanges(
                      system, R2dbcReadJournal.Identifier(), daemonContext.totalProcesses());
              Pair<Integer, Integer> sliceRange = sliceRanges.get(daemonContext.processNumber());
              return ProjectionBehavior.create(createProjection(system, repository, sliceRange));
            },
            ShardedDaemonProcessSettings.create(system),
            Optional.of(ProjectionBehavior.stopMessage()));
  }

  

  private static ExactlyOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> createProjection(
      ActorSystem<?> system,
      ItemPopularityRepository repository,
      Pair<Integer, Integer> sliceRange) {

    int minSlice = sliceRange.first();
    int maxSlice = sliceRange.second();

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider = 
        EventSourcedProvider.eventsBySlices(
            system,
            R2dbcReadJournal.Identifier(), 
            "ShoppingCart",
            minSlice,
            maxSlice);

    String slice = "carts-" + minSlice + "-" + maxSlice;
    Optional<R2dbcProjectionSettings> settings = Optional.empty();
    return R2dbcProjection.exactlyOnce( 
        ProjectionId.of("ItemPopularityProjection", slice),
        settings,
        sourceProvider,
        () -> new ItemPopularityProjectionHandler(slice, repository), 
        system);
  }
}

