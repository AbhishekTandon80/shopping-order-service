package shopping.cart;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.CoordinatedShutdown;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorSystem;
import akka.cluster.MemberStatus;
import akka.cluster.typed.Cluster;
import akka.grpc.GrpcClientSettings;
import akka.testkit.SocketUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.CollectionConverters;
import shopping.cart.proto.*;
import shopping.order.proto.OrderRequest;
import shopping.order.proto.OrderResponse;
import shopping.order.proto.ShoppingOrderService;

public class IntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

  private static Config sharedConfig() {
    return ConfigFactory.load("integration-test.conf");
  }

  private static Config nodeConfig(
      int grcpPort, List<Integer> managementPorts, int managementPortIndex) {
    return ConfigFactory.parseString(
        "shopping-cart-service.grpc.port = "
            + grcpPort
            + "\n"
            + "akka.management.http.port = "
            + managementPorts.get(managementPortIndex)
            + "\n"
            + "akka.discovery.config.services.shopping-cart-service.endpoints = [\n"
            + "  { host = \"127.0.0.1\", port = "
            + managementPorts.get(0)
            + "},\n"
            + "  { host = \"127.0.0.1\", port = "
            + managementPorts.get(1)
            + "},\n"
            + "  { host = \"127.0.0.1\", port = "
            + managementPorts.get(2)
            + "},\n"
            + "]");
  }

  private static class TestNodeFixture {

    private final ActorTestKit testKit;
    private final ActorSystem<?> system;
    private final GrpcClientSettings clientSettings;
    private shopping.cart.proto.ShoppingCartServiceClient client = null;

    public TestNodeFixture(int grcpPort, List<Integer> managementPorts, int managementPortIndex) {
      testKit =
          ActorTestKit.create(
              "IntegrationTest",
              nodeConfig(grcpPort, managementPorts, managementPortIndex)
                  .withFallback(sharedConfig()));
      system = testKit.system();
      clientSettings =
          GrpcClientSettings.connectToServiceAt("127.0.0.1", grcpPort, system).withTls(false);
    }

    public shopping.cart.proto.ShoppingCartService getClient() {
      if (client == null) {
        client = shopping.cart.proto.ShoppingCartServiceClient.create(clientSettings, system);
        CoordinatedShutdown.get(system)
            .addTask(
                CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                "close-test-client-for-grpc",
                () -> client.close());
      }
      return client;
    }
  }

  private static TestNodeFixture testNode1;
  private static TestNodeFixture testNode2;
  private static TestNodeFixture testNode3;
  private static List<ActorSystem<?>> systems;
  private static TestProbe<OrderRequest> orderServiceProbe;
  private static ShoppingOrderService testOrderService;
  private static final Duration requestTimeout = Duration.ofSeconds(10);

  @BeforeClass
  public static void setup() throws Exception {
    List<InetSocketAddress> inetSocketAddresses =
        CollectionConverters.SeqHasAsJava(
                SocketUtil.temporaryServerAddresses(6, "127.0.0.1", false))
            .asJava();
    List<Integer> grpcPorts =
        inetSocketAddresses.subList(0, 3).stream().map(InetSocketAddress::getPort).toList();
    List<Integer> managementPorts =
        inetSocketAddresses.subList(3, 6).stream()
            .map(InetSocketAddress::getPort)
            .collect(Collectors.toList());
    testNode1 = new TestNodeFixture(grpcPorts.get(0), managementPorts, 0);
    testNode2 = new TestNodeFixture(grpcPorts.get(1), managementPorts, 1);
    testNode3 = new TestNodeFixture(grpcPorts.get(2), managementPorts, 2);
    systems = Arrays.asList(testNode1.system, testNode2.system, testNode3.system);
    // create schemas
    CreateTableTestUtils createTableTestUtils = new CreateTableTestUtils(testNode1.system);
    createTableTestUtils.createTables();

    orderServiceProbe = testNode1.testKit.createTestProbe();
    // stub of the ShoppingOrderService
    testOrderService =
        orderRequest -> {
          orderServiceProbe.getRef().tell(orderRequest);
          return CompletableFuture.completedFuture(OrderResponse.newBuilder().setOk(true).build());
        };

    Main.init(testNode1.testKit.system(), testOrderService);
    Main.init(testNode2.testKit.system(), testOrderService);
    Main.init(testNode3.testKit.system(), testOrderService);

    // wait for all nodes to have joined the cluster, become up and see all other nodes as up
    TestProbe<Object> upProbe = testNode1.testKit.createTestProbe();
    systems.forEach(
        system -> {
          upProbe.awaitAssert(
              Duration.ofSeconds(15),
              () -> {
                Cluster cluster = Cluster.get(system);
                assertEquals(MemberStatus.up(), cluster.selfMember().status());
                cluster
                    .state()
                    .getMembers()
                    .iterator()
                    .forEachRemaining(member -> assertEquals(MemberStatus.up(), member.status()));
                return null;
              });
        });
  }

  @AfterClass
  public static void tearDown() {
    testNode3.testKit.shutdownTestKit();
    testNode2.testKit.shutdownTestKit();
    testNode1.testKit.shutdownTestKit();
  }

  @Test
  public void updateAndProjectFromDifferentNodesViaGrpc() throws Exception {
    // add from client1
    CompletionStage<Cart> response1 =
        testNode1
            .getClient()
            .addItem(
                AddItemRequest.newBuilder()
                    .setCartId("cart-1")
                    .setItemId("foo")
                    .setQuantity(42)
                    .build());
    Cart updatedCart1 = response1.toCompletableFuture().get(requestTimeout.getSeconds(), SECONDS);
    assertEquals("foo", updatedCart1.getItems(0).getItemId());
    assertEquals(42, updatedCart1.getItems(0).getQuantity());

    // add from client2
    CompletionStage<Cart> response2 =
        testNode2
            .getClient()
            .addItem(
                AddItemRequest.newBuilder()
                    .setCartId("cart-2")
                    .setItemId("bar")
                    .setQuantity(17)
                    .build());
    Cart updatedCart2 = response2.toCompletableFuture().get(requestTimeout.getSeconds(), SECONDS);
    assertEquals("bar", updatedCart2.getItems(0).getItemId());
    assertEquals(17, updatedCart2.getItems(0).getQuantity());

    // update from client2
    CompletionStage<Cart> response3 =
        testNode2
            .getClient()
            .updateItem(
                UpdateItemRequest.newBuilder()
                    .setCartId("cart-2")
                    .setItemId("bar")
                    .setQuantity(18)
                    .build());
    Cart updatedCart3 = response3.toCompletableFuture().get(requestTimeout.getSeconds(), SECONDS);
    assertEquals("bar", updatedCart3.getItems(0).getItemId());

    // ItemPopularityRepositoryImpl.saveOrUpdate query takes into account of existing count and adds
    // to it.
    assertEquals(18, updatedCart3.getItems(0).getQuantity());

    // ItemPopularityProjection has consumed the events and updated db
    TestProbe<Object> testProbe = testNode1.testKit.createTestProbe();
    testProbe.awaitAssert(
        () -> {
          assertEquals(
              42,
              testNode1
                  .getClient()
                  .getItemPopularity(GetItemPopularityRequest.newBuilder().setItemId("foo").build())
                  .toCompletableFuture()
                  .get(requestTimeout.getSeconds(), SECONDS)
                  .getPopularityCount());

          assertEquals(
              35,
              testNode1
                  .getClient()
                  .getItemPopularity(GetItemPopularityRequest.newBuilder().setItemId("bar").build())
                  .toCompletableFuture()
                  .get(requestTimeout.getSeconds(), SECONDS)
                  .getPopularityCount());
          return null;
        });

    CompletionStage<Cart> response4 =
        testNode2.getClient().checkout(CheckoutRequest.newBuilder().setCartId("cart-2").build());
    Cart cart4 = response4.toCompletableFuture().get(requestTimeout.getSeconds(), SECONDS);
    assertTrue(cart4.getCheckedOut());

    OrderRequest orderRequest = orderServiceProbe.expectMessageClass(OrderRequest.class);
    assertEquals("cart-2", orderRequest.getCartId());
    assertEquals("bar", orderRequest.getItems(0).getItemId());
    assertEquals(18, orderRequest.getItems(0).getQuantity());
  }
}
