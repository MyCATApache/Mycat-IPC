package io.mycat.ipc;

public class Writer {
	public static void main(String[] args) {
		long i = 0;
		long start = System.currentTimeMillis();
		try {
			SharedMMIPMemPool pool = new SharedMMIPMemPool("Coollf.dat3", 1024 * 1024 * 100L, false);
			SharedMMRing ring = pool.getRing((short) 1);

			while (true) {
				i++;
				byte[] data = ("hello           :" + (i++)).getBytes("utf-8");
				while (!ring.putData(data)) {
					Thread.yield();
				}
				//Thread.sleep(2);
//				if(i==100000)
//				{
//					break;
//				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - start);
		System.out.print(i);
	}
}
