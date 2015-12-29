package io.mycat.ipc;

import org.junit.Assert;
import org.junit.Test;

public class TestSharedMMIPMemPool {

	@Test
	public void testInitFromFile() throws Exception {
		SharedMMIPMemPool pool = new SharedMMIPMemPool("testmm.dat", 1024 * 1024 * 100L, true);
		Assert.assertTrue(pool.getQueueCountInMM() == 0);
		SharedMMRing ring = pool.createNewRing((short) 1, 1024 * 1024 * 3);
		QueueMeta queMeta = new QueueMeta((short) 1, 1024 * 1024 * 3, 2 + 6 * 2048);
		Assert.assertEquals(ring.getMetaData(), queMeta);
		for (int i = 0; i < 100; i++) {
			byte[] msg = ("Hellow "+i).getBytes();
			while(!ring.putData(msg))
			{
				Thread.yield();
			}
			System.out.println("add messge "+i);
		}
		//read
		for (int i = 0; i < 100; i++) {
			 
			byte[] msg =ring.pullData();
			if(msg!=null)
			{
				System.out.println("readed messge "+new String(msg));
			}
			
		}
		ring = pool.createNewRing((short) 3, 1024 * 1024 * 2);
		Assert.assertEquals(ring.getMetaData(), new QueueMeta((short) 3, 1024 * 1024 * 2, queMeta.getAddrEnd()));
	}
}
