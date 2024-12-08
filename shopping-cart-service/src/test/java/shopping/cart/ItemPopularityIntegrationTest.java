package shopping.cart;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorSystem;
import akka.cluster.MemberStatus;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.projection.r2dbc.javadsl.R2dbcSession;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import shopping.cart.repository.ItemPopularityRepository;
import shopping.cart.repository.ItemPopularityRepositoryImpl;

public class ItemPopularityIntegrationTest {

  private static Config config() {
    return ConfigFactory.load("item-popularity-integration-test.conf");
  }

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config());

  private static ActorSystem<?> system = testKit.system();
  private static ItemPopularityRepository itemPopularityRepository;

  @BeforeClass
  public static void beforeClass() {

    CreateTableTestUtils utils = new CreateTableTestUtils(system);
    utils.createTables();

    ShoppingCart.init(system);
    itemPopularityRepository = new ItemPopularityRepositoryImpl();
    ItemPopularityProjection.init(system, itemPopularityRepository);

    // form a single node cluster and make sure that completes before running the test
    Cluster node = Cluster.get(system);
    node.manager().tell(Join.create(node.selfMember().address()));

    // let the node join and become Up
    TestProbe<Object> probe = testKit.createTestProbe();
    probe.awaitAssert(
        () -> {
          assertEquals(MemberStatus.up(), node.selfMember().status());
          return null;
        });
  }

  @Test
  public void consumeCartEventsAndUpdatePopularityCount() throws Exception {
    ClusterSharding sharding = ClusterSharding.get(system);

    final String cartId1 = "cart1";
    final String cartId2 = "cart2";
    final String item1 = "item1";
    final String item2 = "item2";

    TestProbe<Object> probe = testKit.createTestProbe();

    EntityRef<ShoppingCart.Command> cart1 = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, cartId1);
    EntityRef<ShoppingCart.Command> cart2 = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, cartId2);

    final Duration timeout = Duration.ofSeconds(3);

    CompletionStage<ShoppingCart.Summary> reply1 =
        cart1.askWithStatus(replyTo -> new ShoppingCart.AddItem(item1, 3, replyTo), timeout);
    ShoppingCart.Summary summary1 = reply1.toCompletableFuture().get(5, SECONDS);
    assertEquals(3, summary1.items().get(item1).intValue());

    probe.awaitAssert(
        Duration.ofSeconds(10),
        () -> {
          Optional<ItemPopularity> item1Popularity =
              R2dbcSession.withSession(
                      system, session -> itemPopularityRepository.findById(session, item1))
                  .toCompletableFuture()
                  .get(3, SECONDS);
          assertTrue(item1Popularity.isPresent());
          assertEquals(3L, item1Popularity.get().count());
          return null;
        });

    CompletionStage<ShoppingCart.Summary> reply2 =
        cart1.askWithStatus(replyTo -> new ShoppingCart.AddItem(item2, 5, replyTo), timeout);
    ShoppingCart.Summary summary2 = reply2.toCompletableFuture().get(5, SECONDS);
    assertEquals(2, summary2.items().size());
    assertEquals(5, summary2.items().get(item2).intValue());
    // another cart
    CompletionStage<ShoppingCart.Summary> reply3 =
        cart2.askWithStatus(replyTo -> new ShoppingCart.AddItem(item2, 4, replyTo), timeout);
    ShoppingCart.Summary summary3 = reply3.toCompletableFuture().get(3, SECONDS);
    assertEquals(1, summary3.items().size());
    assertEquals(4L, summary3.items().get(item2).intValue());

    probe.awaitAssert(
        Duration.ofSeconds(10),
        () -> {
          Optional<ItemPopularity> item2Popularity =
              R2dbcSession.withSession(
                      system, session -> itemPopularityRepository.findById(session, item2))
                  .toCompletableFuture()
                  .get(5, SECONDS);
          assertTrue(item2Popularity.isPresent());
          assertEquals(5 + 4, item2Popularity.get().count());
          return null;
        });
  }

  @Test
  public void safelyUpdatePopularityCount() {
    ClusterSharding sharding = ClusterSharding.get(system);

    final String item = "concurrent-item";
    int cartCount = 30;
    int itemCount = 1;
    final Duration timeout = Duration.ofSeconds(30);

    // Given `item1` is already on the popularity projection DB...
    CompletionStage<ShoppingCart.Summary> rep1 =
        sharding
            .entityRefFor(ShoppingCart.ENTITY_KEY, "concurrent-cart0")
            .askWithStatus(replyTo -> new ShoppingCart.AddItem(item, itemCount, replyTo), timeout);

    TestProbe<Object> probe = testKit.createTestProbe();
    probe.awaitAssert(
        timeout,
        () -> {
          Optional<ItemPopularity> item1Popularity =
              R2dbcSession.withSession(
                      system, session -> itemPopularityRepository.findById(session, item))
                  .toCompletableFuture()
                  .get();
          assertTrue(item1Popularity.isPresent());
          assertEquals(itemCount, item1Popularity.get().count());
          return null;
        });

    // ... when 29 concurrent carts add `item1`...
    for (int i = 1; i < cartCount; i++) {
      sharding
          .entityRefFor(ShoppingCart.ENTITY_KEY, "concurrent-cart" + i)
          .<ShoppingCart.Summary>askWithStatus(
              replyTo -> new ShoppingCart.AddItem(item, itemCount, replyTo), timeout);
    }

    // ... then the popularity count is 30
    probe.awaitAssert(
        timeout,
        () -> {
          Optional<ItemPopularity> item1Popularity =
              R2dbcSession.withSession(
                      system, session -> itemPopularityRepository.findById(session, item))
                  .toCompletableFuture()
                  .get(3, SECONDS);
          assertTrue(item1Popularity.isPresent());
          assertEquals(cartCount * itemCount, item1Popularity.get().count());
          return null;
        });
  }
}
