package io.mycat.ipc;

public class Reader {
	public static void main(String[] args) {
		try {
			SharedMMIPMemPool pool = new SharedMMIPMemPool("Coollf.dat", 1024 * 1024 * 100L, true);
			SharedMMRing ring = pool.getRing((short)1);
			int readed=0;
			while(true){
				byte[] dat = ring.pullData();
				if(dat!=null){
					readed++;
					System.out.println("readed "+readed+" cur msg:"+new String(dat));
				}else{
					Thread.yield();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
