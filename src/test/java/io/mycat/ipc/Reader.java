package io.mycat.ipc;

public class Reader {
	public static void main(String[] args) {
		try {
			SharedMMIPMemPool pool = new SharedMMIPMemPool("Coollf.dat", 1024 * 1024 * 100L, true);
			SharedMMRing ring = pool.createNewRing((short)1,1024 * 1024 * 1);//1MB的内存区域
			while(true){
				byte[] dat = ring.pullData();
				if(dat!=null){
					System.out.println(new String(dat,"utf-8"));
				}else{
					Thread.yield();
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
