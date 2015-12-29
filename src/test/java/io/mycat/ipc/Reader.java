package io.mycat.ipc;

public class Reader {
	public static void main(String[] args) {
		try {
			SharedMMIPMemPool pool = new SharedMMIPMemPool("Coollf.dat", 1024 * 1024 * 100L, true);
			SharedMMRing ring = pool.createNewRing((short)1,1024*1024);
			int readed = 0;
			long curTime = System.currentTimeMillis();
			while (true) {
				byte[] dat = ring.pullData();
				if (dat != null) {
					readed++;
					if (readed % 10000 == 9999) {
						System.out.println(
								"readed " + readed + ",speed " + readed * 1000L / (System.currentTimeMillis() - curTime)
										+ " msgs/second, cur msg:" + new String(dat));
					}
				} else {
					Thread.yield();
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
