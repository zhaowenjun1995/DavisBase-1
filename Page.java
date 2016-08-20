import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	// private static RandomAccessFile davisbaseTablesCatalog;
	// private static RandomAccessFile davisbaseColumnsCatalog;

	public static void main(String[] args){}

	// return payload size in short
	public static short calPayloadSize(String[] values, String[] dataType){
		int val = 1 + dataType.length - 1; // # col + stc - rowid
		for(int i = 1; i < dataType.length; i++){
			String dt = dataType[i];
			switch(dt){
				case "TINYINT":
					val = val + 1;
					break;
				case "SMALLINT":
					val = val + 2;
					break;
				case "INT":
					val = val + 4;
					break;
				case "BIGINT":
					val = val + 8;
					break;
				case "REAL":
					val = val + 4;
					break;		
				case "DOUBLE":
					val = val + 8;
					break;
				case "DATETIME":
					val = val + 8;
					break;
				case "DATE":
					val = val + 8;
					break;
				case "TEXT":
					String text = values[i];
					int len = text.length();
					val = val + len;
					break;
				default:
					break;
			}
		}
		return (short)val;
	}

	// search key by linear return page #
	// public static int searchKey(RandomAccessFile file, int key){

	// }

	// make a new Interior page and return the page number
	public static int makeInteriorPage(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x05);  // this page is a table interior page
		}catch(Exception e){
			System.out.println("Error at makeInteriorPage");
		}

		return num_pages;
	}

	// make a new leaf page and return the page number
	public static int makeLeafPage(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x0D);  // This page is a table leaf page
		}catch(Exception e){
			System.out.println("Error at makeLeafPage");
		}

		return num_pages;

	}

	// return middle key value of the page
	public static int findMidKey(RandomAccessFile file, int page){
		int val = 0;
		try{
			file.seek((page-1)*pageSize);
			byte pageType = file.readByte();
			// num cells in cur page
			int numCells = getCellNumber(file, page);
			// id of mid cell
			int mid = (int) Math.ceil((double) numCells / 2);
			// file.seek((page-1)*pageSize+12+mid*2);
			// short offset = file.readShort(); // get offset of the cell in the page
			// short start = (short) ((page-1)*pageSize);
			// short loc = (short) (start + offset);
			long loc = getCellLoc(file, page, mid-1);
			file.seek(loc);

			switch(pageType){
				case 0x05:
					val = file.readInt(); 
					val = file.readInt();
					break;
				case 0x0D:
					val = file.readShort();
					val = file.readInt();
					break;
			}

		}catch(Exception e){
			System.out.println("Error at findMidKey");
		}

		return val;
	}

	// give half cells in curPage to newPage, and set newPage's parent to curPage's
	public static void splitLeafPage(RandomAccessFile file, int curPage, int newPage){
		try{
			// num cells in cur page
			int numCells = getCellNumber(file, curPage);
			// id of mid cell
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA;
			//short content = 512;
			int content = 512;

			for(int i = numCellA; i < numCells; i++){
				long loc = getCellLoc(file, curPage, i);
				//System.out.println("splitLeafPage loc = "+loc);
				// read cell size
				file.seek(loc);
				//short cellSize = (short)(file.readShort() + 6);
				int cellSize = file.readShort()+6;
				//System.out.println("rowid "+file.readInt()+" ,cell size "+cellSize);
				//System.out.println("current content offset is "+content+" on page "+newPage);
				content = content - cellSize;
				// read cell data
				file.seek(loc);
				byte[] cell = new byte[cellSize];
				file.read(cell);
				// write cell data
				//System.out.println("seeking content offset "+content+" on page "+newPage);
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				// fix cell arrary in the new page TODO
				//short offset = getCellOffset(file, curPage, i);
				setCellOffset(file, newPage, i - numCellA, content);
			}

			// write content offset to new page
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);

			// renew content offset.  TODO delete this. no need to fix
			short offset = getCellOffset(file, curPage, numCellA-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(offset);

			// fix right most pointer
			int rightMost = getRightMost(file, curPage);
			setRightMost(file, newPage, rightMost);
			setRightMost(file, curPage, newPage);

			// fix parent pointer
			int parent = getParent(file, curPage);
			setParent(file, newPage, parent);

			// fix cell number
			byte num = (byte) numCellA;
			setCellNumber(file, curPage, num);
			num = (byte) numCellB;
			setCellNumber(file, newPage, num);
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
			e.printStackTrace();
		}
	}
	// give half cells in curPage to newPage
	public static void splitInteriorPage(RandomAccessFile file, int curPage, int newPage){
		try{
			// num cells in cur page
			//System.out.println("split inter page: "+curPage+" --> "+newPage);
			int numCells = getCellNumber(file, curPage);
			// id of mid cell
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA - 1;
			short content = 512;

			for(int i = numCellA+1; i < numCells; i++){
				long loc = getCellLoc(file, curPage, i);
				// read cell size
				short cellSize = 8;
				content = (short)(content - cellSize);
				// read cell data
				file.seek(loc);
				byte[] cell = new byte[cellSize];
				file.read(cell);
				// write cell data
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				// fix parent pointer in target page
				file.seek(loc);
				int page = file.readInt();
				setParent(file, page, newPage);
				// fix cell arrary in new page
				//short offset = getCellOffset(file, curPage, i);
				setCellOffset(file, newPage, i - (numCellA + 1), content);
			}
			// fix right most pointer in both page
			int tmp = getRightMost(file, curPage);
			setRightMost(file, newPage, tmp);
			//setParent(file, tmp, newPage);
			long midLoc = getCellLoc(file, curPage, mid - 1);
			file.seek(midLoc);
			tmp = file.readInt();
			setRightMost(file, curPage, tmp);
			// write content offset to new page
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);
			// renew content offset.  TODO delete this. no need to fix
			short offset = getCellOffset(file, curPage, numCellA-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(offset);

			// fix parent pointer
			int parent = getParent(file, curPage);
			setParent(file, newPage, parent);
			// fix cell number
			byte num = (byte) numCellA;
			setCellNumber(file, curPage, num);
			num = (byte) numCellB;
			setCellNumber(file, newPage, num);
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
		}
	}

	// split leaf
	public static void splitLeaf(RandomAccessFile file, int page){
		int newPage = makeLeafPage(file);
		int midKey = findMidKey(file, page);
		//System.out.println("split leaf: "+page+" --> "+newPage);
		splitLeafPage(file, page, newPage);
		int parent = getParent(file, page);
		//System.out.println("split leaf: "+page+" old parent "+parent);
		if(parent == 0){
			int rootPage = makeInteriorPage(file);
			setParent(file, page, rootPage);
			//System.out.println("split leaf: set parent "+page+" point to "+rootPage);
			setParent(file, newPage, rootPage);
			//System.out.println("split leaf: set parent "+newPage+" point to "+rootPage);
			setRightMost(file, rootPage, newPage);
			//System.out.println("split leaf: set right most "+rootPage+" point to "+newPage);
			insertInteriorCell(file, rootPage, page, midKey);
		}else{
			long ploc = getPointerLoc(file, page, parent);
			setPointerLoc(file, ploc, parent, newPage);
			insertInteriorCell(file, parent, page, midKey);
			sortCellArray(file, parent);
			while(checkInteriorSpace(file, parent)){
				//System.out.println();
				parent = splitInterior(file, parent);
				//System.out.println("split inter page "+parent);
			}
		}
	}

	// split non leaf
	public static int splitInterior(RandomAccessFile file, int page){
		int newPage = makeInteriorPage(file);
		int midKey = findMidKey(file, page);
		//System.out.println("split inter: "+page+" --> "+newPage);
		splitInteriorPage(file, page, newPage);
		int parent = getParent(file, page);
		//System.out.println("split inter: "+page+" old parent "+parent);
		if(parent == 0){
			int rootPage = makeInteriorPage(file);
			setParent(file, page, rootPage);
			//System.out.println("split inter: set parent "+page+" point to "+rootPage);
			setParent(file, newPage, rootPage);
			//System.out.println("split inter: set parent "+newPage+" point to "+rootPage);
			setRightMost(file, rootPage, newPage);
			//System.out.println("split inter: set right most "+rootPage+" point to "+newPage);
			insertInteriorCell(file, rootPage, page, midKey);
			return rootPage;
		}else{
			long ploc = getPointerLoc(file, page, parent);
			setPointerLoc(file, ploc, parent, newPage);
			//System.out.println("split inter: set pointer "+parent+" point to "+page);
			insertInteriorCell(file, parent, page, midKey);
			sortCellArray(file, parent);
			return parent;
		}
	}

	public static void sortCellArray(RandomAccessFile file, int page){
		 byte num = getCellNumber(file, page);
		 int[] keyArray = getKeyArray(file, page);
		 short[] cellArray = getCellArray(file, page);
		 int ltmp;
		 short rtmp;

		 for (int i = 1; i < num; i++) {
            for(int j = i ; j > 0 ; j--){
                if(keyArray[j] < keyArray[j-1]){

                    ltmp = keyArray[j];
                    keyArray[j] = keyArray[j-1];
                    keyArray[j-1] = ltmp;

                    rtmp = cellArray[j];
                    cellArray[j] = cellArray[j-1];
                    cellArray[j-1] = rtmp;
                }
            }
         }

         try{
         	file.seek((page-1)*pageSize+12);
         	for(int i = 0; i < num; i++){
				file.writeShort(cellArray[i]);
			}
         }catch(Exception e){
         	System.out.println("Error at sortCellArray");
         }
	}

	public static int[] getKeyArray(RandomAccessFile file, int page){
		int num = new Integer(getCellNumber(file, page));
		//System.out.println("Number of cell is: "+num);
		int[] array = new int[num];

		try{
			file.seek((page-1)*pageSize);
			byte pageType = file.readByte();
			byte offset = 0;
			switch(pageType){
				case 0x05:
					offset = 4;
					break;
				case 0x0d:
					offset = 2;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num; i++){
				long loc = getCellLoc(file, page, i);
				//System.out.println("Location is: "+loc);
				file.seek(loc+offset);
				array[i] = file.readInt();
				//System.out.println("has key "+array[i]);
			}

		}catch(Exception e){
			System.out.println("Error at getKeyArray");
		}

		return array;
	}

	public static short[] getCellArray(RandomAccessFile file, int page){
		int num = new Integer(getCellNumber(file, page));
		short[] array = new short[num];

		try{
			file.seek((page-1)*pageSize+12);
			for(int i = 0; i < num; i++){
				array[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println("Error at getCellArray");
		}

		return array;
	}

	// return the parent page number of page
	public static int getParent(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize+8);
			val = file.readInt();
		}catch(Exception e){
			System.out.println("Error at getParent");
		}

		return val;
	}

	public static void setParent(RandomAccessFile file, int page, int parent){
		try{
			file.seek((page-1)*pageSize+8);
			file.writeInt(parent);
			// byte pageType = file.readByte();
			// int numCells = new Integer(file.readByte());
		}catch(Exception e){
			System.out.println("Error at setParent");
		}
	}

	// get pointer location of file in parent page that point to page
	public static long getPointerLoc(RandomAccessFile file, int page, int parent){
		long val = 0;
		try{
			int numCells = new Integer(getCellNumber(file, parent));
			for(int i=0; i < numCells; i++){
				long loc = getCellLoc(file, parent, i);
				//System.out.println("Cell location: "+loc);
				file.seek(loc);
				int childPage = file.readInt();
				//System.out.println("The "+i+" child page number is: "+childPage);
				if(childPage == page){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println("Error at getPointerLoc");
		}

		return val;
	}

	// set pointer in offset point to page
	public static void setPointerLoc(RandomAccessFile file, long loc, int parent, int page){
		try{
			if(loc == 0){
				file.seek((parent-1)*pageSize+4);
			}else{
				file.seek(loc);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println("Error at setPointerLoc");
		}
	} 

	// insert a cell of page/key into no leaf page
	public static void insertInteriorCell(RandomAccessFile file, int page, int child, int key){
		try{
			// find location
			file.seek((page-1)*pageSize+2);
			short content = file.readShort();
			if(content == 0)
				content = 512;
			content = (short)(content - 8);
			// write data
			file.seek((page-1)*pageSize+content);
			file.writeInt(child);
			file.writeInt(key);
			// fix content
			file.seek((page-1)*pageSize+2);
			file.writeShort(content);
			// fix cell arrray
			byte num = getCellNumber(file, page);
			setCellOffset(file, page ,num, content);
			// fix number of cell
			num = (byte) (num + 1);
			setCellNumber(file, page, num);

		}catch(Exception e){
			System.out.println("Error at insertInteriorCell");
		}
	}

	// insert a cell in to leaf page
	public static void insertLeafCell(RandomAccessFile file, int page, int offset, short plsize, int key, byte[] stc, String[] vals){
		try{
			// for(String s: vals){
			// 	System.out.println(s);
			// }
			String s;
			file.seek((page-1)*pageSize+offset);
			//System.out.println("write at "+file.getFilePointer()+" value "+plsize);
			file.writeShort(plsize);
			//System.out.println("write at "+file.getFilePointer()+" value "+key);
			file.writeInt(key);
			int col = vals.length - 1;
			//System.out.println("write at "+file.getFilePointer()+" value "+col);
			file.writeByte(col);
			//System.out.println("stc length: "+stc.length);
			file.write(stc);
			// for(byte s: stc)
			// 	System.out.println(s);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						//System.out.println(vals[i]);
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						// s = vals[i];
						// s = s.substring(1,s.length()-1);
						file.writeBytes(vals[i]);
						//file.writeBytes(s);
						break;
				}
			}
			int n = getCellNumber(file, page);
			byte tmp = (byte) (n+1);
			setCellNumber(file, page, tmp);
			file.seek((page-1)*pageSize+12+n*2);
			file.writeShort(offset);
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*pageSize+2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println("Error at insertLeafCell");
			e.printStackTrace();
		}
	}

	public static void updateLeafCell(RandomAccessFile file, int page, int offset, int plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			//System.out.println("write at "+file.getFilePointer()+" value "+plsize);
			file.writeShort(plsize);
			//System.out.println("write at "+file.getFilePointer()+" value "+key);
			file.writeInt(key);
			int col = vals.length - 1;
			//System.out.println("write at "+file.getFilePointer()+" value "+col);
			file.writeByte(col);
			//System.out.println("stc length: "+stc.length);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						//System.out.println(vals[i]);
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						// s = vals[i];
						// s = s.substring(1,s.length()-1);
						file.writeBytes(vals[i]);
						//file.writeBytes(s);
						break;
				}
			}
		}catch(Exception e){
			System.out.println("Error at Page.update");
			System.out.println(e);
		}
	}

	public static int getRightMost(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize+4);
			val = file.readInt();
		}catch(Exception e){
			System.out.println("Error at getRightMost");
		}

		return val;
	}

	public static void setRightMost(RandomAccessFile file, int page, int rightMost){

		try{
			file.seek((page-1)*pageSize+4);
			file.writeInt(rightMost);
		}catch(Exception e){
			System.out.println("Error at setRightMost");
		}

	}

	// return number of cells in a page
	public static byte getCellNumber(RandomAccessFile file, int page){
		byte val = 0;

		try{
			//System.out.println("page :"+page);
			file.seek((page-1)*pageSize+1);
			val = file.readByte();
			//System.out.println("cell num :"+val);
		}catch(Exception e){
			System.out.println(e);
			System.out.println("Error at getCellNumber");
		}

		return val;
	}

	public static void setCellNumber(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*pageSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println("Error at setCellNumber");
		}
	}

	// assmue interior page has 10 more key implys full
	public static boolean checkInteriorSpace(RandomAccessFile file, int page){
		byte numCells = getCellNumber(file, page);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	// 
	public static int checkLeafSpace(RandomAccessFile file, int page, int size){
		int val = -1;

		try{
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content == 0)
				return pageSize - size;
			int numCells = getCellNumber(file, page);
			int space = content - 20 - 2*numCells;
			if(size < space)
				return content - size;
			// else{
			// 	short[] array = getCellArray(file, page);
			// 	for(short offsetA : array){
			// 		file.seek((page-1)*pageSize+offsetA);
			// 		int cellSize = 2+4+file.readShort();
			// 		int endOffset = offsetA + cellSize + size;
			// 		boolean flag = true;
			// 		for(short offsetB : array)
			// 			if((offsetB > offsetA) && (offsetB < endOffset)){
			// 				flag = false;
			// 				break;
			// 			}
			// 		if(flag)
			// 			return offsetA + cellSize;
			// 	}
			
		}catch(Exception e){
			System.out.println("Error at checkLeafSpace");
		}

		return val;
	}

	// if the page has the key, return true, otherwise return false
	public static boolean hasKey(RandomAccessFile file, int page, int key){
		int[] array = getKeyArray(file, page);
		for(int i : array)
			if(key == i)
				return true;
		return false;
	}

	//public static void rewriteData(RandomAccessFile file, int page, String value, String dateType, int key, int colPos){}

	// read loaction of cell in the page, id starts at 0
	public static long getCellLoc(RandomAccessFile file, int page, int id){
		//System.out.println("Getting cell location of page "+page+" child "+id);
		long loc = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			short offset = file.readShort();
			long orig = (page-1)*pageSize;
			loc = orig + offset;
			//System.out.println("Cell location: "+loc);
		}catch(Exception e){
			System.out.println("Error at getCellLoc");
		}
		return loc;
	}

	public static short getCellOffset(RandomAccessFile file, int page, int id){
		short offset = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			offset = file.readShort();
			//System.out.println("Cell location: "+loc);
		}catch(Exception e){
			System.out.println("Error at getCellOffset");
		}
		return offset;
	}

	public static void setCellOffset(RandomAccessFile file, int page, int id, int offset){
		try{
			file.seek((page-1)*pageSize+12+id*2);
			file.writeShort(offset);
			//System.out.println("Cell location: "+loc);
		}catch(Exception e){
			System.out.println("Error at setCellOffset");
		}
	}
}















