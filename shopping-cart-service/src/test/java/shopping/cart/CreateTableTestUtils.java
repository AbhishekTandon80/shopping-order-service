package shopping.cart;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.persistence.r2dbc.ConnectionFactoryProvider;
import akka.persistence.r2dbc.internal.R2dbcExecutor;
import akka.projection.r2dbc.R2dbcProjectionSettings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.*;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.jdk.javaapi.FutureConverters;

public class CreateTableTestUtils {

  private final File createTablesFile = Paths.get("ddl-scripts/create_tables.sql").toFile();
  private final File dropTablesFile = Paths.get("ddl-scripts/drop_tables.sql").toFile();
  private final File createUserTablesFile =
      Paths.get("ddl-scripts/create_user_tables.sql").toFile();

  private final R2dbcProjectionSettings r2dbcProjectionSettings;
  private final R2dbcExecutor r2dbcExecutor;

  final String testConfigPath = "akka.projection.r2dbc";

  final ActorSystem<?> actorSystem;

  public CreateTableTestUtils(ActorSystem<?> system) {
    this.actorSystem = system;
    this.r2dbcProjectionSettings =
        R2dbcProjectionSettings.apply(actorSystem.settings().config().getConfig(testConfigPath));

    this.r2dbcExecutor =
        new R2dbcExecutor(
            new ConnectionFactoryProvider(actorSystem)
                .connectionFactoryFor(r2dbcProjectionSettings.useConnectionFactory()),
            LoggerFactory.getLogger(getClass()),
            r2dbcProjectionSettings.logDbCallsExceeding(),
            actorSystem.executionContext(),
            actorSystem);
  }

  public void createTables() {
    try {
      CompletableFuture<Done> dropTableFuture = executeDdl("beforeAll dropTable", dropTablesFile);
      dropTableFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

      CompletableFuture<Done> createTableFuture =
          executeDdl("beforeAll createTable", createTablesFile);
      createTableFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

      CompletableFuture<Done> createUsersTableFuture =
          executeDdl("beforeAll createUsersTable", createUserTablesFile);
      createUsersTableFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      e.printStackTrace();
    }
    LoggerFactory.getLogger(CreateTableTestUtils.class).info("Tables created");
  }

  private CompletableFuture<Done> executeDdl(String message, File file) {
    Future<Done> scalaFuture =
        r2dbcExecutor.executeDdl(
            message,
            connection -> {
              try {
                return connection.createStatement(tablesSql(file));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    CompletionStage<Done> javaCompletionStage = FutureConverters.asJava(scalaFuture);
    return javaCompletionStage.toCompletableFuture();
  }

  private String tablesSql(File file) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(System.lineSeparator());
      }
    }
    return sb.toString();
  }

  /**
   * Drops and create user tables (if applicable)
   *
   * <p>This file is only created on step 4 (projection query). Calling this method has no effect on
   * previous steps.
   */
  //  public void dropTables() {
  //
  //    try {
  //      CompletableFuture<Done> dropTableFuture = executeDdl("beforeAll dropTable",
  // dropTablesFile);
  //      dropTableFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
  //
  //    } catch (InterruptedException | ExecutionException | TimeoutException e) {
  //      e.printStackTrace();
  //    }
  //    LoggerFactory.getLogger(CreateTableTestUtils.class).info("Tables dropped");
  //    }
}
