package io.mycat.ipc;

import org.junit.Test;

public class TestBase {

	@Test
	public void testbitOp()

	{

		int size = 1024;

		 if (Integer.bitCount(size) != 1)
	        {
	            throw new IllegalArgumentException("bufferSize must be a power of 2");
	        }
		int indexMask = size - 1;
		int index = 5;
		int nexIndex = (index & indexMask);
		System.out.println(nexIndex);
		index = 1024;
		nexIndex = (index & indexMask);
		System.out.println(nexIndex);
		index = 1025;
		nexIndex = (index & indexMask);
		System.out.println(nexIndex);
	}

}
