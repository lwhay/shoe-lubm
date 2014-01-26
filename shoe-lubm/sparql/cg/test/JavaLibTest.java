/**
 * 
 */
package cg.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cg.cache.Block;
import cg.cache.BlockHeader;
import cg.trie.Factory;

/**
 * @author C_G
 *
 */
public class JavaLibTest {
	static void testPattern() {
		String str = "UndergraduateStudent164@Department0.University0.edu";
		str = str.replaceAll("UndergraduateStudent", " a");
		str = str.replace("Department", " b");
		str = str.replace("University", " c");
		str = str.replace("edu", " d");
		str = "helll$fsfd ";
		System.out.println(str.trim().split("\\$")[0]);
		String str1 = "<http://www.Department0.University0.edu/UndergraduateStudent164>";
		Pattern p = Pattern.compile("<http://www.Department[0-1]\\d*.University\\d+\\.edu.*>");
		Matcher m = p.matcher(str1);
		if(m.find()) {
			System.out.println(m.group());
			System.out.println(m.replaceAll(""));
		}
	}
	static void testThreadRead() throws IOException {
		File file = new File("rdf.nt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		System.out.println(br.readLine());
		System.out.println(br1.readLine());
		System.out.println(br.readLine());
		System.out.println(br1.readLine());
		System.out.println(br.readLine());
		System.out.println(br1.readLine());
	}
	static void testMappedByteBuffer() throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File("index"), "rw");
//		file.setLength(0);
		FileChannel fchannel = file.getChannel();
	
//		MappedByteBuffer mapping = fchannel.map(MapMode.READ_WRITE, 0, Block.SIZE);
//		InterNode inode0 = new InterNode(KeyAnalyzer.ENDMARK);
//		inode0.child((byte)36, 100);
		System.out.println(file.length());
//		byte[] bytes = inode0.getBytes();
//		System.out.println(bytes.length);
//		file.write(bytes);
		BlockHeader header = new BlockHeader();
		try {
			ByteBuffer buff = fchannel.map(MapMode.READ_WRITE, 6184, Block.SIZE);
			header.deserializer(buff);
			System.out.println(header.seek() + " "+ header.headType());
			Block blk = Factory.createNode(header);
			blk.deserialize(buff);
			System.out.println(blk.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
//		System.out.println(mapping.capacity());
//		System.out.println(mapping.position());
//		System.out.println(file.length());
	}
	static void testByteBuffer() throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File("testfile"), "rw");
		file.setLength(0);
		FileChannel fchannel = file.getChannel();
//		byte[] b = {1,2,3,4,5,6,7,8,9,0};
		int size = (Integer.SIZE + Long.SIZE) / 8;
		ByteBuffer in = ByteBuffer.allocate(size);
		
		in.putInt(999);
		in.putLong(-999);
		if (in.hasArray()) {
			byte[] bytes = in.array();
			System.out.println(bytes.length);
			for (int i = 0; i < bytes.length; i++) 
				System.out.print(bytes[i] + " ");
			System.out.println();
			file.seek(0);
			file.write(bytes);
//			ByteBuffer out = ByteBuffer.wrap(bytes);
			ByteBuffer out = fchannel.map(MapMode.READ_ONLY, 0, size+10);
			System.out.println(out.getInt());
			System.out.println(out.getLong());
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		testMappedByteBuffer();
//		testByteBuffer();
//		testThreadRead();
		testPattern();
	}

}
