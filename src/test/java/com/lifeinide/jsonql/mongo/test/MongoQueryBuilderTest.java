package com.lifeinide.jsonql.mongo.test;

import com.lifeinide.jsonql.core.dto.BasePageableRequest;
import com.lifeinide.jsonql.core.dto.Page;
import com.lifeinide.jsonql.core.test.JsonQLBaseQueryBuilderTest;
import com.lifeinide.jsonql.mongo.DefaultMongoFilterQueryBuilder;
import com.lifeinide.jsonql.mongo.MongoFilterQueryBuilder;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.LocalDate;
import java.util.function.BiConsumer;

import static org.bson.codecs.configuration.CodecRegistries.*;

/**
 * @author Lukasz Frankowski
 */
public class MongoQueryBuilderTest extends JsonQLBaseQueryBuilderTest<
	MongoCollection<MongoEntity>,
	ObjectId,
	MongoEntity,
	MongoFilterQueryBuilder<MongoEntity, Page<MongoEntity>>
> {

	private MongodExecutable mongodExecutable = null;
	private MongoCollection<MongoEntity> collection = null;

	@BeforeAll
	public void init() throws Exception {
		MongodStarter starter = MongodStarter.getDefaultInstance();

		String bindIp = "localhost";
		int port = 12345;
		IMongodConfig mongodConfig = new MongodConfigBuilder()
			.version(Version.Main.PRODUCTION)
			.net(new Net(bindIp, port, Network.localhostIsIPv6()))
			.build();

		mongodExecutable = starter.prepare(mongodConfig);
		MongodProcess mongod = mongodExecutable.start();
		MongoClientSettings settings = null;

		MongoClient mongo = MongoClients.create(MongoClientSettings.builder()
				.applyConnectionString(new ConnectionString("mongodb://" + bindIp + ":" + port))
				.codecRegistry(fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
					fromProviders(PojoCodecProvider.builder().automatic(true).build())))
				.build());
		collection = mongo.getDatabase("test").getCollection("test", MongoEntity.class);

		populateData(entity -> collection.insertOne((MongoEntity) entity));
	}

	@AfterAll
	public void done() {
		try {
			if (mongodExecutable != null)
				mongodExecutable.stop();
		} finally {
			mongodExecutable = null;
		}
	}

	@Nonnull
	@Override
	protected MongoEntity buildEntity(ObjectId previousId) {
		return new MongoEntity();
	}

	@Nullable
	@Override
	protected Object buildAssociatedEntity() {
		return null;
	}

	@Override
	protected void doTest(BiConsumer<MongoCollection<MongoEntity>, MongoFilterQueryBuilder<MongoEntity, Page<MongoEntity>>> c) {
		c.accept(collection, new DefaultMongoFilterQueryBuilder<>(collection));
	}

	@Test
	public void testDefaultSortOrder() {
		doTest((pc, qb) -> {
			qb.context().getSorts().add(Sorts.descending("dateVal"));
			Assertions.assertEquals(LocalDate.of(2018, 4, 10), qb
				.list(BasePageableRequest.ofDefault().withPageSize(1))
				.getData().iterator().next().getDateVal());
		});

	}

}
