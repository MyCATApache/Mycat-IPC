package io.mycat.ipc.util;

/**
 * tools
 * 
 * @author wuzhih
 *
 */
public class Util {
	public static long roundTo4096(long i) {
		return (i + 0xfffL) & ~0xfffL;
	}
	public static void  main(String[] args)
	{
		System.out.println(roundTo4096(4095));
	}
}
