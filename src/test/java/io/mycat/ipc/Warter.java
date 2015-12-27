package io.mycat.ipc;

public class Warter {
	public static void main(String[] args) {
		int i = 0; 
		long start =  System.currentTimeMillis();
		try {
			SharedMMIPMemPool pool = new SharedMMIPMemPool("Coollf.dat", 1024 * 1024 * 100L, true);
			SharedMMRing ring = pool.createNewRing((short)1,1024 * 1024 * 1);//1MB的内存区域
			
			while(true){				
				ring.addData(("hello"+(i++)).getBytes("utf-8") );
				Thread.sleep(2);
//				if(System.currentTimeMillis() - start > 1000){
//					break;
//				}
			}
//			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - start);
		System.out.print(i);
	}
}
