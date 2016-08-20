import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;

	public static void main(String[] args){}


	public static void show(){
		String[] cols = {"table_name"};
		String[] cmp = new String[0];
		String table = "davisbase_tables";
		select(table, cols, cmp);
	}

	public static void drop(String table){
		try{
			// clear meta-table
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Page.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Page.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						//System.out.println(tb);
						if(!tb.equals(table)){
							Page.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNumber(file, page, (byte)i);
				}
			}

			// clear meta-column
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Page.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Page.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						//System.out.println(tb);
						if(!tb.equals(table)){
							Page.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNumber(file, page, (byte)i);
				}
			}

			//delete file
			File anOldFile = new File("data", table+".tbl"); 
			anOldFile.delete();
		}catch(Exception e){
			System.out.println("Error at drop");
			System.out.println(e);
		}

	}

	public static String[] retrievePayload(RandomAccessFile file, long loc){
		String[] payload = new String[0];
		try{
			Long tmp;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);

			// get stc
			file.seek(loc);
			int plsize = file.readShort();
			int key = file.readInt();
			int num_cols = file.readByte();
			byte[] stc = new byte[num_cols];
			int temp = file.read(stc);
			payload = new String[num_cols+1];
			payload[0] = Integer.toString(key);
			// get payLoad
			//file.seek(loc+7+num_cols);
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println("Error at retrievePayload");
		}

		return payload;
	}


	public static void createTable(String table, String[] col){
		try{	
			//System.out.println("Creating table "+ table);
			//System.out.println("Insert record into table");
			//file
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			// table
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			//System.out.println("davisbase_tables.tbl pages: "+numPages);
			int page = 1;
			for(int p = 1; p <= numPages; p++){
				int rm = Page.getRightMost(file, p);
				if(rm == 0)
					page = p;
			}
			//System.out.println("davisbase_tables.tbl right most page: "+page);
			int[] keyArray = Page.getKeyArray(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
				if(l < keyArray[i])
					l = keyArray[i];
			file.close();
			String[] values = {Integer.toString(l+1), table};
			insertInto("davisbase_tables", values);


			//System.out.println("Insert record into column");
			// colums
			//file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			//numPages = pages(file);
			//System.out.println("column pages: "+numPages);
			// page = 1;
			// for(int p = 1; p <= numPages; p++){
			// 	int rm = Page.getRightMost(file, p);
			// 	if(rm == 0)
			// 		page = p;
			// }
			// //System.out.println("target page :"+page);
			// keyArray = Page.getKeyArray(file, page);

			// if(keyArray.length != 0){
			// 	l = keyArray[0];
			// }else{
			// 	int loc = Page.getCellLoc(file, page, 0);
			// 	file.seek(loc);
			// 	l = file.readInt();
			// 	l = file.readInt();
			// }
			//file.close();

			// for(int i = 0; i < keyArray.length; i++)
			// 	if(l < keyArray[i])
			// 		l = keyArray[i];


			RandomAccessFile cfile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(cfile, cmp, columnName, buffer);
			l = buffer.content.size();
			//System.out.println(l);


			//System.out.println("l in column :"+l);
			for(int i = 0; i < col.length; i++){
				//System.out.println("in the for loop");
				l = l + 1;
				//System.out.println("l in column :"+l);
				String[] token = col[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String[] v = {Integer.toString(l), table, col_name, dt, pos, n};
				insertInto("davisbase_columns", v);
			}
			file.close();
		}catch(Exception e){
			System.out.println("Error at createTable");
			e.printStackTrace();
		}
	}

	public static void update(String table, String[] set, String[] cmp){
		try{
			int key = new Integer(cmp[2]);
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int numPages = pages(file);
			int page = 1;

			for(int p = 1; p <= numPages; p++)
				if(Page.hasKey(file, p, key)){
					page = p;
				}
			//System.out.println("update on page: "+ page);
			int[] array = Page.getKeyArray(file, page);
			int id = 0;
			for(int i = 0; i < array.length; i++)
				if(array[i] == key)
					id = i;
			int offset = Page.getCellOffset(file, page, id);
			long loc = Page.getCellLoc(file, page, id);
			String[] array_s = getColName(table);
			int num_cols = array_s.length - 1;
			//String[] values = retrievePayload(file, loc, key, num_cols);
			String[] values = retrievePayload(file, loc);
			// for(String s: values)
			// 	System.out.println(s);


			// fix date time type value to string format before update
			String[] type = getDataType(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";


			// update value on a column
			for(int i = 0; i < array_s.length; i++)
				if(array_s[i].equals(set[0]))
					id = i;
			values[id] = set[2];
			// for(String s: values)
			// 	System.out.println(s);

			// check null value violation
			String[] nullable = getNullable(table);

			for(int i = 0; i < nullable.length; i++){
				//System.out.print(values[i]+" "+nullable[i]);
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL value constraint violation");
					System.out.println();
					return;
				}
			}



			//check uniqueness violation
			// for(int p = 1; p <= numPages; p++)
			// 	if(Page.hasKey(file, p, key)){
			// 		page = p;
			// }

			byte[] stc = new byte[array_s.length-1];
			int plsize = calPayloadSize(table, values, stc);
			Page.updateLeafCell(file, page, offset, plsize, key, stc, values);

			file.close();

		}catch(Exception e){
			System.out.println("Error at update");
			System.out.println(e);
		}
	}

	public static void insertInto(RandomAccessFile file, String table, String[] values){
		String[] dtype = getDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL value constraint violation");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
			if(Page.hasKey(file, page, key)){
				System.out.println("Uniqueness constraint violation");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		//System.out.println("check point");
		byte[] stc = new byte[dtype.length-1];
		short plSize = (short) calPayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Page.checkLeafSpace(file, page, cellSize);

		//System.out.println("Insert into offset "+offset);

		if(offset != -1){
			//System.out.println(key+" key write to page :"+page);
			Page.insertLeafCell(file, page, offset, plSize, key, stc, values);
			//Page.sortCellArray(file, page);
		}else{
			//System.out.println("splite page"+page);
			Page.splitLeaf(file, page);
			insertInto(file, table, values);
		}
	}

	public static void insertInto(String table, String[] values){
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			insertInto(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println("Error at insertInto table");
			e.printStackTrace();
		}
	}

	public static int calPayloadSize(String table, String[] vals, byte[] stc){
		String[] dataType = getDataType(table);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++){
			byte tmp = stcCode(vals[i], dataType[i]);
			stc[i - 1] = tmp;
			size = size + feildLength(tmp);
		}
		return size;
	}

	//calculate value length by stc
	public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	// return STC
	public static byte stcCode(String val, String dataType){
		if(val.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key){
		int val = 1;
		//System.out.println("search key "+ key);
		try{
			int numPages = pages(file);
			//System.out.println("num page: "+numPages);
			for(int page = 1; page <= numPages; page++){
				//System.out.println("searching page: "+page);
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					//System.out.println("Page "+page+" is a leaf");
					int[] keys = Page.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						//System.out.println("Page "+page+" return");
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						//System.out.println("Page "+page+" return");
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println("Error at searchKey");
			System.out.println(e);
		}

		return val;
	}


	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}catch(Exception e){
			System.out.println("Error at getDataType");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColName(String table){
		String[] c = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}catch(Exception e){
			System.out.println("Error at getColName");
			System.out.println(e);
		}
		return c;
	}

	public static String[] getNullable(String table){
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("Error at getNullable");
			System.out.println(e);
		}
		return n;
	}

	public static void select(String table, String[] cols, String[] cmp){
		try{
			Buffer buffer = new Buffer();
			//System.out.println("select "+table);
			String[] columnName = getColName(table);
			String[] type = getDataType(table);
			// String test = "";
			// for(String s: columnName)
			// 	test = test + s + ", ";
			// System.out.println(table+": "+test);

			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			filter(file, cmp, columnName, type, buffer);
			buffer.display(cols);
			file.close();
		}catch(Exception e){
			System.out.println("Error at select");
			System.out.println(e);
		}
	}

	// filter fuction for select
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Buffer buffer){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNumber(file, page);

					for(int i=0; i < numCells; i++){
						//System.out.println("check point");
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); // seek to rowid
						int rowid = file.readInt(); // read rowid
						int num_cols = new Integer(file.readByte()); // read # of columns other than rowid
						//String[] payload = new String[num_cols + 1];
						//String[] payload = retrievePayload(file, loc, rowid, num_cols); // retrieve payLoad and convert to strings
						String[] payload = retrievePayload(file, loc);

						// String test = "";
						// for(String s: payload)
						// 	test = test + s +", ";
						// System.out.println("payload: "+test);

						// fix date time type value to string format in the payload before check
						// String[] type = getDataType(table);
						// String test = "";
						// for(String s: type)
						// 	test = test + s +", ";
						// System.out.println("type: "+test);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = "'"+payload[j]+"'";
						// check
						boolean check = cmpCheck(payload, rowid, cmp, columnName);

						// convert back date type
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = payload[j].substring(1, payload[j].length()-1);

						if(check)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			//System.out.println(e);
			e.printStackTrace();
		}

	}

	// filter function for getDT getNull
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, Buffer buffer){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNumber(file, page);

					for(int i=0; i < numCells; i++){
						//System.out.println("check point");
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); // seek to rowid
						int rowid = file.readInt(); // read rowid
						int num_cols = new Integer(file.readByte()); // read # of columns other than rowid
						//String[] payload = new String[num_cols + 1];
						//String[] payload = retrievePayload(file, loc, rowid, num_cols); // retrieve payLoad and convert to strings
						String[] payload = retrievePayload(file, loc);

						// String test = "";
						// for(String s: payload)
						// 	test = test + s +", ";
						// System.out.println("payload: "+test);

						boolean check = cmpCheck(payload, rowid, cmp, columnName);
						if(check)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			//System.out.println(e);
			e.printStackTrace();
		}

	}

	// return number of page of the file
	public static int pages(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println("Error at makeInteriorPage");
		}

		return num_pages;
	}

	// retrieve binary payload and parse into list of string, loc is cell location in the file
	// num_cols is #col other than rowid
	// public static String[] retrievePayload(RandomAccessFile file, int loc, int rowid, int num_cols){
	// 	String[] payload = new String[num_cols+1];
	// 	payload[0] = Integer.toString(rowid);
	// 	byte[] stc = new byte[num_cols+1];
	// 	stc[0] = 0;

	// 	try{
	// 		// get stc
	// 		file.seek(loc+7);
	// 		for(int i=1; i <= num_cols; i++){
	// 			stc[i] = file.readByte();
	// 		}

	// 		// get payLoad
	// 		file.seek(loc+7+num_cols);
	// 		for(int i=1; i <= num_cols; i++){
	// 			switch(stc[i]){
	// 				case 0x00:  payload[i] = "null";
	// 							break;

	// 				case 0x01:  payload[i] = "null";
	// 							break;

	// 				case 0x02:  payload[i] = "null";
	// 							break;

	// 				case 0x03:  payload[i] = "null";
	// 							break;

	// 				case 0x04:  payload[i] = Integer.toString(file.readByte());
	// 							break;

	// 				case 0x05:  payload[i] = Integer.toString(file.readShort());
	// 							break;

	// 				case 0x06:  payload[i] = Integer.toString(file.readInt());
	// 							break;

	// 				case 0x07:  payload[i] = Long.toString(file.readLong());
	// 							break;

	// 				case 0x08:  payload[i] = String.valueOf(file.readFloat());
	// 							break;

	// 				case 0x09:  payload[i] = String.valueOf(file.readDouble());
	// 							break;

	// 				case 0x0A:  payload[i] = "DATETIME";
	// 							break;

	// 				case 0x0B:  payload[i] = "DATE";
	// 							break;

	// 				default:    int len = new Integer(stc[i]-0x0C);
	// 							byte[] bytes = new byte[len];
	// 							for(int j = 0; j < len; j++)
	// 								bytes[j] = file.readByte();
	// 							payload[i] = new String(bytes);
	// 							break;
	// 			}
	// 		}

	// 	}catch(Exception e){
	// 		System.out.println("Error at retrievePayload2 rowid :"+rowid);
	// 	}

	// 	return payload;
	// }

	// check if a row satisfy the filter condition
	public static boolean cmpCheck(String[] payload, int rowid, String[] cmp, String[] columnName){

		//System.out.println("Checking cmp on row: "+ rowid);
		boolean check = false;
		if(cmp.length == 0){
			//System.out.println("No cmp require");
			check = true;
		}else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			//System.out.println("Cmp is "+cmp[0]+" "+cmp[1]+" "+cmp[2]);
			if(colPos == 1){
				//System.out.println("checing rowid");
				switch(opt){
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  // TODO: check the operator
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				//System.out.println("chech other column");
				if(val.equals(payload[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		// if(check)
		// 	System.out.println("Cmp pass");
		// else
		// 	System.out.println("Cmp fail");

		return check;
	}

	public static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		try {
			davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);// page type
			davisbaseTablesCatalog.write(0x02);// num cell
			int[] offset=new int[2];
			int size1=24;//table size
			int size2=25;// column size
			offset[0]=pageSize-size1;
			offset[1]=offset[0]-size2;
			davisbaseTablesCatalog.writeShort(offset[1]);// content offset
			davisbaseTablesCatalog.writeInt(0);// rightmost
			davisbaseTablesCatalog.writeInt(10);// parent
			davisbaseTablesCatalog.writeShort(offset[1]);// cell arrary 1
			davisbaseTablesCatalog.writeShort(offset[0]);// cell arrary 2
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(20);
			davisbaseTablesCatalog.writeInt(1); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(28);
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(21);
			davisbaseTablesCatalog.writeInt(2); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(29);
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
		try {
			davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       
			davisbaseColumnsCatalog.writeByte(0x0D); // page type: leaf page
			davisbaseColumnsCatalog.writeByte(0x08); // number of cells
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbaseColumnsCatalog.writeShort(offset[8]); // content offset
			davisbaseColumnsCatalog.writeInt(0); // rightmost
			davisbaseColumnsCatalog.writeInt(0); // parent
			// cell array
			for(int i=0;i<9;i++)
				davisbaseColumnsCatalog.writeShort(offset[i]);

			// data
			davisbaseColumnsCatalog.seek(offset[0]);
			davisbaseColumnsCatalog.writeShort(33); // 34
			davisbaseColumnsCatalog.writeInt(1); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables"); // 16
			davisbaseColumnsCatalog.writeBytes("rowid"); // 5
			davisbaseColumnsCatalog.writeBytes("INT"); // 3
			davisbaseColumnsCatalog.writeByte(1); // 1
			davisbaseColumnsCatalog.writeBytes("NO"); // 2
			//davisbaseColumnsCatalog.writeBytes("PRI");
			
			davisbaseColumnsCatalog.seek(offset[1]);
			davisbaseColumnsCatalog.writeShort(39); // 38
			davisbaseColumnsCatalog.writeInt(2); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables"); // 16
			davisbaseColumnsCatalog.writeBytes("table_name"); // 10  
			davisbaseColumnsCatalog.writeBytes("TEXT"); // 4
			davisbaseColumnsCatalog.writeByte(2); // 1
			davisbaseColumnsCatalog.writeBytes("NO"); // 2
			//davisbaseColumnsCatalog.writeByte(0);
			
			davisbaseColumnsCatalog.seek(offset[2]);
			davisbaseColumnsCatalog.writeShort(34); // 35
			davisbaseColumnsCatalog.writeInt(3); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeBytes("PRI");
			
			davisbaseColumnsCatalog.seek(offset[3]);
			davisbaseColumnsCatalog.writeShort(40); // 39
			davisbaseColumnsCatalog.writeInt(4); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeByte(0);

			
			davisbaseColumnsCatalog.seek(offset[4]);
			davisbaseColumnsCatalog.writeShort(41); // 40
			davisbaseColumnsCatalog.writeInt(5); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeByte(0);
			
			davisbaseColumnsCatalog.seek(offset[5]);
			davisbaseColumnsCatalog.writeShort(39); // 38
			davisbaseColumnsCatalog.writeInt(6); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeByte(0);
			
			davisbaseColumnsCatalog.seek(offset[6]);
			davisbaseColumnsCatalog.writeShort(49); // 48
			davisbaseColumnsCatalog.writeInt(7); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeByte(0);
			
			davisbaseColumnsCatalog.seek(offset[7]);
			davisbaseColumnsCatalog.writeShort(41); // 40
			davisbaseColumnsCatalog.writeInt(8); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			//davisbaseColumnsCatalog.writeByte(0);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");
			//davisbaseColumnsCatalog.writeByte(0);
			
			// davisbaseColumnsCatalog.seek(offset[8]);
			// davisbaseColumnsCatalog.writeShort(41); // 40
			// davisbaseColumnsCatalog.writeInt(9); 
			// davisbaseColumnsCatalog.writeByte(5);
			// davisbaseColumnsCatalog.writeByte(29);
			// davisbaseColumnsCatalog.writeByte(22);
			// davisbaseColumnsCatalog.writeByte(16);
			// davisbaseColumnsCatalog.writeByte(4);
			// davisbaseColumnsCatalog.writeByte(15);
			// //davisbaseColumnsCatalog.writeByte(0);
			// davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			// davisbaseColumnsCatalog.writeBytes("column_key");
			// davisbaseColumnsCatalog.writeBytes("TEXT");
			// davisbaseColumnsCatalog.writeByte(7);
			// davisbaseColumnsCatalog.writeBytes("YES");
			// //davisbaseColumnsCatalog.writeByte(0);
			
			// RandomAccessFile ram=davisbaseColumnsCatalog;
			// System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			// ram.seek(0);
			// long size = ram.length();
			// int row = 1;
			// System.out.print("0000\t0x0000\t");
			// while(ram.getFilePointer() < size) {
			// 	System.out.print(String.format("%02X ", ram.readByte()));
			// 	// System.out.print(ram.readByte() + " ");
			// 	if(row % 16 == 0) {
			// 		System.out.println();
			// 		System.out.print(String.format("%04d\t0x%04X\t", row, row));
			// 	}
			// 	row++;
			// }
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}


// store and display information
class Buffer{
	public int num_row;
	public HashMap<Integer, String[]> content;
	//public String[] col;
	public int[] format;
	public String[] columnName;

	public Buffer(){
		num_row = 0;
		//num_col = num_col;
		//col_name = new String[num_col];
		content = new HashMap<Integer, String[]>();
	}

	public void add(int rowid, String[] val){
		content.put(rowid, val);
		num_row = num_row + 1;
		// TODO: update format function
	}

	public void updateFormat(){
		for(int i = 0; i < format.length; i++)
			format[i] = columnName[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
		}
	}

	public String fix(int len, String s){
		return String.format("%-"+(len+3)+"s", s);
		//return s;
	}

	public String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}

	// display content according to the format
	public void display(String[] col){
		if(num_row == 0){
			System.out.println("Empty set.");
		}else{
			updateFormat();
			if(col[0].equals("*")){
				// print line
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();
				// print name
				for(int j = 0; j < columnName.length; j++)
					System.out.print(fix(format[j], columnName[j])+"|");
				System.out.println();
				// print line
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();
				// print data
				for(String[] i : content.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
				System.out.println();
			}else{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;
				// print line
				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				// print name
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], columnName[control[j]])+"|");
				System.out.println();
				// print line
				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				// print data
				for(String[] i : content.values()){
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}