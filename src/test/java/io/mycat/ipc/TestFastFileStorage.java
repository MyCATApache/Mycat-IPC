package io.mycat.ipc;

import org.junit.Test;

public class TestFastFileStorage {

	@Test
	public void testWriteAndRead() throws Exception {
		FastestFileStorage st = new FastestFileStorage("mm.data", 100 * 1024 * 1024L, true, 1024 * 1024);
		for (int i = 0; i < 100000; i++) {
			String data = new String(" hellow " + i);
			int writeResult = st.writeData(data.getBytes());
			if (writeResult == -1) {
				System.out.println("full " + i);
				break;
			} else if (writeResult == 0) {
				System.out.println("cant't write " + i);
				break;
			} else {
				if (i % 1000 == 0)
					System.out.println("write success " + i);
			}
		}
		st.close();
		System.out.println(" closed  ");

		st = new FastestFileStorage("mm.data", 100 * 1024 * 1024L, false, 1024 * 1024);

	}
}
