import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;



/**
 * @author Chris Irwin Davis
 * @version 1.0
 * <b>This is an example of how to read/write binary data files using RandomAccessFile class</b>
 *
 */
public class DavisBase {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {
    	//Table.initializeDataStore();
    	init();
    	//Table.initializeDataStore();
		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!userCommand.equals("exit")) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}// end main method

	///////////////////////////////////////////////////////////////////////////////////////////////
	//
	//  Method definitions
	//

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		// version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
		System.out.println("\tVERSION;                                         Show the program version.");
		System.out.println("\tHELP;                                            Show this help information");
		System.out.println("\tEXIT;                                            Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	/** Display the DavisBase version */
	public static void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}

	// if table does not exist, return false, else return true
	public static boolean tableExist(String table){
		boolean e = false;
		table = table+".tbl";
		try {
			File dataDir = new File("data");
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				if(oldTableFiles[i].equals(table))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		return e;
	}

	public static void init(){
		try {
			File dataDir = new File("data");
			if(dataDir.mkdir()){
				System.out.println("System dir 'data' does not exit, initializing data base...");
				System.out.println();
				Table.initializeDataStore();
			}else {
				String meta1 = "davisbase_columns.tbl";
				String meta2 = "davisbase_tables.tbl";
				String[] oldTableFiles = dataDir.list();
				boolean check = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals(meta1))
						check = true;
				}
				if(!check){
					System.out.println("System table 'davisbase_columns.tbl' does not exit, initializing data base...");
					System.out.println();
					Table.initializeDataStore();
				}
				check = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals(meta2))
						check = true;
				}
				if(!check){
					System.out.println("System table 'davisbase_tables.tbl' does not exit, initializing data base...");
					System.out.println();
					Table.initializeDataStore();
				}
			}
		}catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

	}



	public static String[] parserEquation(String equ){
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if(equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}// end parseEquation
		
	public static void parseUserCommand (String userCommand) {
		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specifi)c
		 * method inside each case statement */
		String[] commandTokens = userCommand.split(" ");

		switch (commandTokens[0]) {
			case "init":
				Table.initializeDataStore();
				break;

			case "create":
				String create_table = commandTokens[2];
				String[] create_temp = userCommand.split(create_table);
				String col_temp = create_temp[1].trim();
				String[] create_cols = col_temp.substring(1, col_temp.length()-1).split(",");
				for(int i = 0; i < create_cols.length; i++)
					create_cols[i] = create_cols[i].trim();
				if(tableExist(create_table)){
					System.out.println("Table "+create_table+" already exists.");
					System.out.println();
					break;
				}
				// System.out.println(create_table);
				// for(String s: create_cols)
				// 	System.out.println(s);
				Table.createTable(create_table, create_cols);		
				break;

			case "drop":
				String tb = commandTokens[2];
				if(!tableExist(tb)){
					System.out.println("Table "+tb+" does not exist.");
					System.out.println();
					break;
				}
				Table.drop(tb);
				break;

			case "show":
				Table.show();
				break;

			case "insert":
				String insert_table = commandTokens[2];
				String insert_vals = userCommand.split("values")[1].trim();
				insert_vals = insert_vals.substring(1, insert_vals.length()-1);
				String[] insert_values = insert_vals.split(",");
				for(int i = 0; i < insert_values.length; i++)
					insert_values[i] = insert_values[i].trim();
				if(!tableExist(insert_table)){
					System.out.println("Table "+insert_table+" does not exist.");
					System.out.println();
					break;
				}
				// System.out.println(insert_table);
				// for(String s: insert_values)
				// 	System.out.println(s);
				Table.insertInto(insert_table, insert_values);
				break;

			case "update":
				String update_table = commandTokens[1];
				String[] update_temp1 = userCommand.split("set");
				String[] update_temp2 = update_temp1[1].split("where");
				String update_cmp_s = update_temp2[1];
				String update_set_s = update_temp2[0];
				String[] set = parserEquation(update_set_s);
				String[] update_cmp = parserEquation(update_cmp_s);
				if(!tableExist(update_table)){
					System.out.println("Table "+update_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.update(update_table, set, update_cmp);
				break;
				
			case "select":
				String[] select_cmp;
				String[] select_column;
				String[] select_temp = userCommand.split("where");
				if(select_temp.length > 1){
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				}else{
					select_cmp = new String[0];
				}
				String[] select = select_temp[0].split("from");
				String select_table = select[1].trim();
				String select_cols = select[0].replace("select", "").trim();
				if(select_cols.contains("*")){
					select_column = new String[1];
					select_column[0] = "*";
				}
				else{
					select_column = select_cols.split(",");
					for(int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if(!tableExist(select_table)){
					System.out.println("Table "+select_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.select(select_table, select_column, select_cmp);
				break;

			case "help":
				help();
				break;

			case "version":
				version();
				break;

			case "exit":
				break;

			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				System.out.println();
				break;
		}
	} // end parseUserCommand
	
}