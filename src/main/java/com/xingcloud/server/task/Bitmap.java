package com.xingcloud.server.task;

import java.util.Arrays;
import java.util.Random;

/**
 * User: IvyTang
 * Date: 13-4-12
 * Time: 下午4:42
 */
public class Bitmap {

  // todo: re-implement using java.util.BitSet

  private byte[] bits = new byte[1024];
  private static final int[] masks = new int[]{1,2,4,8,16,32,64,128};
  private static final int[] ormasks = new int[]{0xff-1,0xff-2,0xff-4,0xff-8,0xff-16,0xff-32,0xff-64,0xff-128};

  private long MAX_LENGTH = 10*1024*1024;//80M bits, 10M bytes

  private long lower = 5000000l;//a large value, to be reset

  public boolean get(long id){
    int offset = (int) ((id - lower) >> 3);
    return !(offset >= bits.length || offset < 0) && (bits[offset] & masks[((int) (id & 7))])  != 0;
  }

  public void set(long id, boolean is){
    if(id >= lower && id < lower + (bits.length << 3)){
      int offset = (int) ((id - lower) >> 3);
      if(is){
        bits[offset] = (byte) (bits[offset] | masks[((int) (id & 7))]);
        offset = 0;
      }else{
        bits[offset] = (byte) (bits[offset] & ormasks[((int) (id & 7))]);
      }
    }else if(id < lower){
      //lower expand
      long newlower = id - (id & 7);

      int newlength = (int) ((lower - newlower)>>3) + bits.length;
      if ( newlength > MAX_LENGTH){
        return;
      }
      byte newbits[] = new byte[newlength];
      System.arraycopy(bits,0,newbits,newlength-bits.length,bits.length);
      //System.out.println(""+bits.length+" bytes copied.");
      if(is){
        newbits[0] = (byte) (newbits[0] | masks[((int) (id & 7))]);
      }else{
        newbits[0] = (byte) (newbits[0] & ormasks[((int) (id & 7))]);
      }
      bits = newbits;
      lower = newlower;
    }else if(id >= lower + (bits.length << 3)){
      //upper expand
      int newlength = (int) ( ((id - lower) >> 3) + 1);
      newlength = newlength > bits.length * 2 ? newlength: bits.length * 2;
      if(newlength > MAX_LENGTH){
        return;
      }
      byte newbits[] = new byte[newlength];
      System.arraycopy(bits,0,newbits,0,bits.length);
      //System.out.println(""+bits.length+" bytes copied.");
      bits=newbits;
      int offset = (int) ((id - lower) >> 3);
      if(is){
        bits[offset] = (byte) (bits[offset] | masks[((int) (id & 7))]);
        offset = 0;
      }else{
        bits[offset] = (byte) (bits[offset] & ormasks[((int) (id & 7))]);
      }
    }

  }

  public static void main(String[] args) {
    test(args);
  }

  public static void test(String[] args) {
    Bitmap bitmap= new Bitmap();
    Random rand = new Random();
    for (int i = 0; i < 10000000; i++) {
      long id = rand.nextInt(3000000);
      bitmap.set(id, true);
      if(!bitmap.get(id)){
        System.err.println("error true!"+id);
      }
      bitmap.set(id,false);
      if(bitmap.get(id)){
        System.err.println("error false!"+id);
      }
    }
    byte[] results = new byte[10000];
    testSerial(bitmap, results);
    testSerial(bitmap, results);
    long t1 = System.currentTimeMillis();
    testSerial(bitmap, results);
    long t2 = System.currentTimeMillis();
    System.out.println(t2-t1);
    for (int i = 0; i < results.length; i++) {
      byte result = results[i];
      if (result == 1 && !bitmap.get(i)){
        System.err.println("error serial true!");
      }else if (result == 2 && bitmap.get(i)){
        System.err.println("error serial false!");
      }
    }
    for (int i = 0; i < 1000; i++) {
      bitmap.set(i, true);
    }
  }

  private static void testSerial(Bitmap bitmap, byte[] results) {
    Random rand = new Random();
    for (int i = 0; i < 10000000; i++) {
      long id = i;//rand.nextInt(results.length);
      boolean hit = true;//rand.nextBoolean();
      bitmap.set(id, hit);
      //bitmap.get(id);
			/*
			if(hit){
				results[((int) id)] = 1;
			}else{
				results[((int) id)] = 2;//false
			}
			if(bitmap.get(id)!=hit){
				System.err.println("error random !"+id);
			}                        */
    }
  }

  public void reset() {
    Arrays.fill(bits, (byte) 0);
  }
}
