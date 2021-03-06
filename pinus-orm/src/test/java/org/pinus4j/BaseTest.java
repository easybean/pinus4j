package org.pinus4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.api.ShardingStorageClientImpl;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.exceptions.LoadConfigException;

public class BaseTest {

	public static final String CLUSTER_KLSTORAGE = "pinus";

	public static final String CACHE_HOST = "127.0.0.1:11211";

	protected static final Random r = new Random();

	protected static final String[] seeds = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };

	public static String getContent(int len) {
		StringBuilder content = new StringBuilder();
		for (int i = 0; i < len; i++) {
			content.append(seeds[r.nextInt(9)]);
		}
		return content.toString();
	}

	public static TestEntity createEntity() {
		TestEntity testEntity = new TestEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setOTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) 255);
		testEntity.setOTestByte((byte) 255);
		testEntity.setTestChar('a');
		testEntity.setOTestChar('a');
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(1.0);
		testEntity.setOTestDouble(1.0);
		testEntity.setTestFloat(2.0f);
		testEntity.setOTestFloat(2.0f);
		testEntity.setTestInt(5);
		testEntity.setOTestInt(5);
		testEntity.setTestLong(6l);
		testEntity.setOTestLong(6l);
		testEntity.setTestShort((short) 7);
		testEntity.setOTestShort((short) 7);
		testEntity.setTestString(getContent(r.nextInt(100)));
		testEntity.setTestTime(new Timestamp(System.currentTimeMillis()));
		return testEntity;
	}

	public static TestGlobalEntity createGlobalEntity() {
		TestGlobalEntity testEntity = new TestGlobalEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setoTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) 255);
		testEntity.setoTestByte((byte) 255);
		testEntity.setTestChar('b');
		testEntity.setoTestChar('b');
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(1.0);
		testEntity.setoTestDouble(1.0);
		testEntity.setTestFloat(2.0f);
		testEntity.setoTestFloat(2.0f);
		testEntity.setTestInt(5);
		testEntity.setoTestInt(5);
		testEntity.setTestLong(6l);
		testEntity.setoTestLong(6l);
		testEntity.setTestShort((short) 7);
		testEntity.setoTestShort((short) 7);
		testEntity.setTestString(getContent(r.nextInt(100)));
		testEntity.setTestTime(new Timestamp(System.currentTimeMillis()));
		return testEntity;
	}

	public static IShardingStorageClient getStorageClient() {
		IShardingStorageClient storageClient = new ShardingStorageClientImpl();

		storageClient.setScanPackage("org.pinus4j");
		storageClient.setSyncAction(EnumSyncAction.UPDATE);
		try {
			storageClient.init();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}

		return storageClient;
	}

	// @Test
	public void genData() throws Exception {
		IShardingStorageClient storageClient = getStorageClient();
		
		List<TestEntity> dataList = new ArrayList<TestEntity>(3000);
		int i = 0;
		while (true) {
			dataList.add(createEntity());
			if (i++ % 3000 == 0) {
				Number[] pks = storageClient.saveBatch(dataList,
						new ShardingKey<Integer>(CLUSTER_KLSTORAGE, r.nextInt(60000000)));
				System.out.println("save " + pks.length);
				dataList.clear();
			}
		}
	}

}
