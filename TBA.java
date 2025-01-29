/**
 * %W% %E% Gireesh B.
 *
 * Copyright (c) 2012 Aon Hewitt Limited
 *
 * This software is the confidential and proprietary information of AON
 * Hewitt ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with AON Hewitt.
 *
 **/
package com;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lib.AppVariables;
import lib.DBUtil;
import lib.Queries;

/**
 * @author A0181784
 */
public final class TBA {

	/** Initializing the dBUname object. **/
	private static String dBUname = null;
	/** Initializing the dBPwd object. **/
	private static String dBPwd = null;
	/** Initializing the dbUtil object. **/
	private static DBUtil dbUtil = null;
	/** Initializing the ResultSet object. **/
	private static ResultSet rs = null;
	/** Initializing the TENTHOUSAND object. **/
	private static final int TENTHOUSAND = 10000;
	/** Initializing the THREE object. **/
	private static final int THREE = 3;
	/** Initializing the fileResendMap object. **/
	private static Map<String, String> fileResendMap = new HashMap<String, String>();
	/** Initializing the valMessage object. **/
	private static String valMessage = null;
	/** Initializing the runtimePath object. **/
	private static String extn = ".txt";
	/** Initializing the runtimePath object. **/
	private static String runtimePath = "./runtime/";
	/** Initializing the countTotalRecord object. **/
	private static int countTotalRecord = 0;
	/** Initializing the crtUserId object. */
	private static String crtUserId = AppVariables.get("Crt_User_ID").trim();
	/** Initializing the queriesFile object. **/
	private static String queriesFile = AppVariables.get("Queries_File").trim();
	/** Initializing the schema name object. **/
	private static String schemaName = AppVariables.get("Schema_Name").trim();
	/** Initializing the clientId object. **/
	private static String clientId = null;
	/** Initializing the keyColumnsMap object. */
	private static LinkedHashMap<String, LinkedHashMap<Integer, String>> keyOutputMap = new LinkedHashMap<String, LinkedHashMap<Integer, String>>();
	/** Initializing the AccountId object. */
	private static String accountId = null;
	/** Initializing the SecGrpId object. */
	private static StringBuffer secGrpId = new StringBuffer();
	/** Initializing the outputFileList object. **/
	private static List<String> outputFileList = null;
	/** Initializing the mCountryMap object. **/
	private static HashMap<String, String> mCountryMap = new HashMap<String, String>();
	/** Contains details about the fixed length columns. **/
	private static HashMap<Integer, Integer> fileColsMap = new HashMap<Integer, Integer>();
	/** Initializing delimiter. **/
	private static String delimiter = null;
	/** Initializing WorkCountry. **/
	private static String WorkCountry = null;
	/** Initializing HomeState. **/
	private static String homeState = null;
	/** Contains no of columns. **/
	private static Integer noOfColumns = 0;

	/** Initializing Constructor. **/
	private TBA() {
	}

	/**
	 * This is the main method.
	 *
	 * @param args is null
	 */
	public static void main(final String[] args) {
		try {
			if (!validateConfig()) {
				System.out.println(valMessage);
				System.exit(1);
			}
			System.out.println("Validation is completed for input parameters.");
			getCDSConnection();
			loadCountryMap();
			for (int i = 0; i < outputFileList.size(); i++) {
				String fileName = outputFileList.get(i);
				validateKeyColumnPos(fileName);
			}

			for (int i = 0; i < outputFileList.size(); i++) {
				String fileName = outputFileList.get(i);
				System.out.println("\nProcess is started for file " + fileName + " at " + new Date());
				renameCurrFile(fileName);
				createCurrFile(fileName);
				getResendRecord(fileName);
				System.out.println("Current file is created at " + new Date() + ".");
				createOutputFiles(fileName);
				System.out.println("Output file is created at " + new Date() + ".");
				if (fileResendMap.size() > 0) {
					updateResendTbl(fileName);
				}
				fileResendMap.clear();
				System.out.println("Process is completed for file " + fileName + " at " + new Date() + "\n");
			}
			dbUtil.getConnection().commit();
			System.out.println("CDS database connection is committed successfully.");

			FileWriter tempFileWriter = new FileWriter(new File("./runtime/Record_Count.txt"));
			tempFileWriter.write(countTotalRecord + "");
			tempFileWriter.close();
			closeCDSConnection();

		} catch (Exception e1) {
			System.out.println("Exception: " + e1 + "\n" + Arrays.toString(e1.getStackTrace()));
			e1.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * This method is used to validate the key column position.
	 *
	 * @param fileName name of file
	 * @throws Exception throw exception
	 */
	private static void validateKeyColumnPos(final String fileName) throws Exception {
		String query;
		try {
			query = Queries.getQuery(fileName + "_SELECT_QUERY");
			if (query == null) {
				System.out.println(
						"The query " + fileName + "_SELECT_QUERY does not exist " + "in the " + queriesFile + ".");

				closeCDSConnection();
				System.exit(2);
			}
			query = query.replace(":1:", schemaName).replace(":2:", clientId).replace(":3:", " 1")
					.replace(":4:", WorkCountry).replace(":hs:", homeState);
			query = query.replace("1=1", "1=2");
			rs = dbUtil.runSelectQuery(query, false);
			rs.setFetchSize(TENTHOUSAND);
			ResultSetMetaData rsmd = rs.getMetaData();

			int totalClmCount = rsmd.getColumnCount();
			LinkedHashMap<Integer, String> keyColumnsMap = keyOutputMap.get(fileName);
			Iterator<Integer> keyMap = keyColumnsMap.keySet().iterator();
			while (keyMap.hasNext()) {
				int temp = keyMap.next();
				if (temp > totalClmCount) {
					System.out.println("The value of the position specified for " + "the parameter " + fileName
							+ "_Key_Columns_Positions in the Application.props"
							+ " should be less than or equal to the number" + " of column(s) in the query " + fileName
							+ "_SELECT_QUERY.");

					closeCDSConnection();
					System.exit(1);
				} else {
					continue;
				}
			}
		} catch (Exception e) {
			System.out.println("Exception while validating the key column position: " + e + "\n"
					+ Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(1);
		}

	}

	/**
	 * This method is used to rename the current file into previous file.
	 *
	 * @param fileName input file name
	 * @throws Exception throw exception
	 */
	private static void renameCurrFile(final String fileName) throws Exception {
		try {
			String fileType = AppVariables.get(fileName + "_File_Type").trim();

			if (new File(runtimePath + "Curr_" + fileName + extn).exists()) {
				new File(runtimePath + "Curr_" + fileName + extn)
				.renameTo(new File(runtimePath + "Prev_" + fileName + extn));
			} else {
				if (fileType.equalsIgnoreCase("C")) {
					System.out.println("The value of the parameter " + fileName + "_"
							+ "File_Type in the Application.props is 'C', "
							+ "but the previous run's current file does not exist.");
					closeCDSConnection();
					System.exit(1);
				}
			}
		} catch (Exception e) {
			System.out.println(
					"Exception while renaming the current file: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(1);
		}
	}

	/**
	 * This method is used to validate the input parameters.
	 *
	 * @return true/false.
	 */
	public static boolean validateConfig() {
		// DB_UserName
		try {
			dBUname = AppVariables.get("DB_UserName").trim();
			if ((dBUname.equals(""))) {
				valMessage = "The value of the parameter DB_UserName " + "in the Application.props should be valid.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter DB_UserName " + "does not exist in the Application.props.";
			return false;
		}
		// DB_Password_SECURE
		try {
			dBPwd = AppVariables.get("DB_Password_SECURE").trim();
			if ((dBPwd.equals(""))) {
				valMessage = "The value of the parameter DB_Password_SECURE "
						+ "in the Application.props should be valid.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter DB_Password_SECURE " + "does not exist in the Application.props.";
			return false;
		}

		// Client_ID
		try {
			clientId = AppVariables.get("Client_ID").trim();
			if (clientId.equals("")) {
				valMessage = "The value of the parameter Client_ID " + "in the Application.props should be valid.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter Client_ID " + "does not exist in the Application.props.";
			return false;
		}

		// Output_FileList
		try {
			if (AppVariables.get("Output_FileList").trim().equals("")) {
				valMessage = "The value of the parameter Output_FileList "
						+ "in the Application.props should be valid.";
				return false;
			} else {
				String[] output = AppVariables.get("Output_FileList").split(",", -1);
				for (int i = 0; i < output.length; i++) {
					output[i] = output[i].trim();
				}
				outputFileList = Arrays.asList(output);
			}
		} catch (Exception e) {
			valMessage = "The parameter Output_FileList " + "does not exist in the Application.props.";
			return false;
		}
		// File_Columns
		try {
			String fileCols = AppVariables.get("Details_Columns").trim();
			if (fileCols.equals("")) {
				valMessage = "The parameter Details_Columns " + "in the Application.props must contain a valid value.";
				return false;
			} else if (fileCols.endsWith(",")) {
				valMessage = "The value for parameter Details_Columns "
						+ "in the Application.props cannot end with comma.";
				return false;
			}
			String[] fileColsArr = fileCols.split(",", -1);
			noOfColumns = fileColsArr.length;
			int i = 1;
			for (String cols : fileColsArr) {

				String[] colArr = cols.split(":", -1);
				if (colArr.length > 2) {
					valMessage = "The parameter Details_Columns "
							+ "in the Application.props must contain a valid value.";
					return false;
				}

				for (String colArrVal : colArr) {
					if (colArrVal.equals("")) {
						valMessage = "The column size/position of parameter"
								+ " File_Columns must contain a valid value in the " + "Application.props.";
						return false;
					}
				}
				Integer key = null;
				Integer value = null;
				try {
					key = Integer.parseInt(cols.split(":", -1)[0].trim());
					value = Integer.parseInt(cols.split(":", -1)[1].trim());
				} catch (NullPointerException ne) {
					valMessage = "The parameter Details_Columns "
							+ "in the Application.props must contain a valid value.";
					return false;
				}

				if (i != key) {
					valMessage = "Index " + i + " does not exist in the Application.props "
							+ "for the parameter Details_Columns.";
					return false;
				}
				fileColsMap.put(key, value);
				i++;
			}
		} catch (ArrayIndexOutOfBoundsException aie) {
			valMessage = "The parameter Details_Columns " + "in the Application.props must contain a valid value.";
			return false;
		} catch (NumberFormatException e1) {
			valMessage = "The parameter Details_Columns should only"
					+ " contain numeric values in the Application.props.";
			return false;
		} catch (Exception e) {
			valMessage = "The parameter Details_Columns " + "does not exist in the Application.props.";
			return false;
		}
		// Delimiter
		try {
			delimiter = AppVariables.get("Delimiter").trim();
			if (delimiter.equals("")) {
				valMessage = "The parameter Delimiter " + "in the Application.props must contain a valid value.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter Delimiter " + "does not exist in the Application.props.";
			return false;
		}
		// Work_country
		try {
			WorkCountry = AppVariables.get("Work_country").trim();
			if (WorkCountry.equals("")) {
				valMessage = "The parameter Work_country " + "in the Application.props must contain a valid value.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter Work_country " + "does not exist in the Application.props.";
			return false;
		}

		// Home_State
		try {
			homeState = AppVariables.get("Home_State").trim();
			if (homeState.equals("")) {
				valMessage = "The parameter Home_State " + "in the Application.props must contain a valid value.";
				return false;
			}
		} catch (Exception e) {
			valMessage = "The parameter Home_State " + "does not exist in the Application.props.";
			return false;
		}

		return validateOutputConfig();
	}

	/**
	 * This method is used to validate the input parameters.
	 *
	 * @return true/false.
	 */
	public static boolean validateOutputConfig() {
		String fileName;
		for (int i = 0; i < outputFileList.size(); i++) {
			fileName = outputFileList.get(i);

			// Output_File
			try {
				if (AppVariables.get(fileName + "_Output_File").trim().equals("")) {
					valMessage = "The value of the parameter " + fileName + "_Output_File "
							+ "in the Application.props should be valid.";
					return false;
				}
			} catch (Exception e) {
				valMessage = "The parameter " + fileName + "_Output_File " + "does not exist in the Application.props.";
				return false;
			}
			// Min_Required_Record_Count
			try {
				String temp = AppVariables.get(fileName + "_Min_Required_Record_Count").trim();
				if (temp.equals("")) {
					valMessage = "The value of the parameter " + fileName + "_Min_Required_Record_Count"
							+ "in the Application.props should be valid.";
					return false;
				}
				if (Long.parseLong(temp) <= 0) {
					valMessage = "The parameter " + fileName + "_Min_Required_Record_Count should be > 0.";
					return false;
				}
			} catch (NumberFormatException e1) {
				valMessage = "The value of the parameter " + fileName + "_Min_Required_Record_Count"
						+ " in the Application.props should be numeric.";
				return false;
			} catch (Exception e) {
				valMessage = "The parameter " + fileName + "_Min_Required_Record_Count does "
						+ "not exist in the Application.props.";
				return false;
			}
			// File_Type
			try {
				String fileType = AppVariables.get(fileName + "_File_Type").trim();
				if (fileType.equals("")) {
					valMessage = "The value of the parameter " + fileName + "_File_Type "
							+ "in the Application.props should be valid.";
					return false;
				} else if (!fileType.equalsIgnoreCase("F") && !fileType.equalsIgnoreCase("C")) {
					valMessage = "The value of the parameter " + fileName + "_File_Type "
							+ "in the Application.props should be valid.";
					return false;
				}
			} catch (Exception e) {
				valMessage = "The parameter " + fileName + "_File_Type " + "does not exist in the Application.props.";
				return false;
			}
			// Max_Change_Count
			try {
				String temp = AppVariables.get(fileName + "_Max_Change_Count").trim();
				if (temp.equals("")) {
					valMessage = "The value of the parameter " + fileName + "_Max_Change_Count"
							+ " in the Application.props should be valid.";
					return false;
				}
				if (Integer.parseInt(temp) <= 0) {
					valMessage = "The value of the parameter " + fileName + "_Max_Change_Count"
							+ " in the Application.props should be > 0.";
					return false;
				}
			} catch (NumberFormatException e1) {
				valMessage = "The value of the parameter " + fileName + "_Max_Change_Count"
						+ " in the Application.props should be an integer.";
				return false;
			} catch (Exception e) {
				valMessage = "The parameter " + fileName + "_Max_Change_Count"
						+ " does not exist in the Application.props.";
				return false;
			}
			// Key_Columns_Positions
			try {
				String tempKeyColumns = AppVariables.get(fileName + "_Key_Columns").trim();
				LinkedHashMap<Integer, String> keyColumnsMap = new LinkedHashMap<Integer, String>();
				if (!tempKeyColumns.equals("")) {
					String[] arrTemp = AppVariables.get(fileName + "_Key_Columns").split("\\,", -1);
					for (String keyColumns : arrTemp) {
						String[] arrTempA = keyColumns.split("\\:", -1);
						if (Integer.parseInt(arrTempA[1].trim()) < 1) {
							valMessage = "The value of the position specified for the " + "parameter " + fileName
									+ "_Key_Columns" + " in the Application.props should be > 0.";
							return false;
						}
						keyColumnsMap.put(Integer.parseInt(arrTempA[1].trim()), arrTempA[0].trim());
					}
					keyOutputMap.put(fileName, keyColumnsMap);
				} else {
					valMessage = "The value of the parameter " + fileName + "_Key_Columns "
							+ "in the Application.props should be valid.";
					return false;
				}
			} catch (NumberFormatException e) {
				valMessage = "The value of the parameter " + fileName + "_Key_Columns"
						+ " in the Application.props should be numeric.";
				return false;
			} catch (Exception e) {
				valMessage = "The parameter " + fileName + "_Key_Columns " + "does not exist in the Application.props.";
				return false;
			}
		}
		return true;
	}

	/**
	 * This method is used to get the key value from the line.
	 *
	 * @param currLine input line
	 * @param fileName input file name
	 * @return key value
	 * @throws Exception throws exception if any to the calling method.
	 */
	private static String getKeyValue(final String currLine, final String fileName) throws Exception {
		String[] lineArray;
		String line = currLine;
		lineArray = line.split("\\" + delimiter, -1);

		String key = "";
		LinkedHashMap<Integer, String> keyColumnsMap = keyOutputMap.get(fileName);
		Iterator<Integer> keyMap = keyColumnsMap.keySet().iterator();
		while (keyMap.hasNext()) {
			int temp = keyMap.next();
			if (!key.equals("")) {
				key = key + delimiter;
			}

			key = key + lineArray[temp - 1];

		}
		return key;
	}

	/**
	 * This method is used to compare the previous file and current file.
	 *
	 * @param fileName input file name
	 * @throws Exception throws exception if any to the calling method.
	 */
	private static void createOutputFiles(final String fileName) throws Exception {
		Date date = new Date();
		SimpleDateFormat sdf1 = new SimpleDateFormat("MMddyyyy");

		String outputFile = AppVariables.get(fileName + "_Output_File").trim();
		String fileType = AppVariables.get(fileName + "_File_Type").trim();

		BufferedReader currFileRead = null;
		BufferedReader prevFileRead = null;
		BufferedWriter outputFileWrite = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream("./runtime/" + outputFile)));

		String currLine = null;
		String prevLine = null;
		try {

			outputFileWrite.write("0101698PepsiCo" + sdf1.format(date)
			+ "                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             ");
			outputFileWrite.newLine();

			currFileRead = new BufferedReader(
					new InputStreamReader(new FileInputStream(runtimePath + "Curr_" + fileName + extn)));

			if (fileType.equalsIgnoreCase("C")) {
				prevFileRead = new BufferedReader(
						new InputStreamReader(new FileInputStream(runtimePath + "Prev_" + fileName + extn)));
				currLine = currFileRead.readLine();
				prevLine = prevFileRead.readLine();

				while (currLine != null || prevLine != null) {
					if (currLine != null && prevLine != null && !(currLine.equals(prevLine))) {
						String currKey = getKeyValue(currLine, fileName);
						String prevKey = getKeyValue(prevLine, fileName);
						if (currKey.compareTo(prevKey) == 0) {
							String resendKey = fileName + delimiter;
							if (fileResendMap.containsKey(resendKey)) {
								fileResendMap.put(resendKey, "Y");
							}
							String[] CurrArray = currLine.split("\\" + delimiter, -1);
							String[] PrevArray = prevLine.split("\\" + delimiter, -1);
							String tempC = currLine.replace(delimiter + CurrArray[14] + delimiter,
									delimiter + "" + delimiter);
							String tempP = prevLine.replace(delimiter + PrevArray[14] + delimiter,
									delimiter + "" + delimiter);
							if (!tempC.equalsIgnoreCase(tempP)) {
								outputFileWrite.write(writeInToFile(currLine));
								outputFileWrite.newLine();
								countTotalRecord++;
							}
							currLine = currFileRead.readLine();
							prevLine = prevFileRead.readLine();
							// countTotalRecord++;
							continue;
						} else if (currKey.compareTo(prevKey) < 0) {
							String resendKey = fileName + delimiter;
							// Insert
							if (fileResendMap.containsKey(resendKey)) {
								fileResendMap.put(resendKey, "Y");
							}
							outputFileWrite.write(writeInToFile(currLine));
							outputFileWrite.newLine();
							currLine = currFileRead.readLine();
							countTotalRecord++;
							continue;
						} else if (currKey.compareTo(prevKey) > 0) {
							// outputFileWrite.write(writeInToFile(prevLine));
							// outputFileWrite.newLine();
							prevLine = prevFileRead.readLine();
							// countTotalRecord++;
							continue;
						} else {
							String resendKey = fileName + delimiter;
							if (fileResendMap.containsKey(resendKey)) {
								fileResendMap.put(resendKey, "Y");
								outputFileWrite.write(writeInToFile(currLine));
								outputFileWrite.newLine();
								countTotalRecord++;
							}
							currLine = currFileRead.readLine();
							prevLine = prevFileRead.readLine();
							continue;
						}
					} else if (currLine != null && prevLine != null && currLine.equals(prevLine)) {
						String currKey = getKeyValue(currLine, fileName);
						String resendKey = fileName + delimiter + currKey;
						if (fileResendMap.containsKey(resendKey)) {
							fileResendMap.put(resendKey, "Y");
							outputFileWrite.write(writeInToFile(currLine));
							outputFileWrite.newLine();
							countTotalRecord++;
						}
						currLine = currFileRead.readLine();
						prevLine = prevFileRead.readLine();
						continue;
					} else if (prevLine == null && currLine != null) {
						String currKey = getKeyValue(currLine, fileName);
						String resendKey = fileName + delimiter + currKey;
						if (fileResendMap.containsKey(resendKey)) {
							fileResendMap.put(resendKey, "Y");
						}
						outputFileWrite.write(writeInToFile(currLine));
						outputFileWrite.newLine();
						currLine = currFileRead.readLine();
						countTotalRecord++;
						continue;
					} else if (prevLine != null && currLine == null) {
						// outputFileWrite.write(writeInToFile(prevLine));
						// outputFileWrite.newLine();
						prevLine = prevFileRead.readLine();
						// countTotalRecord++;
						continue;
					} else {
						String currKey = getKeyValue(currLine, fileName);
						String resendKey = fileName + delimiter + currKey;
						if (fileResendMap.containsKey(resendKey)) {
							fileResendMap.put(resendKey, "Y");
							outputFileWrite.write(writeInToFile(currLine));
							outputFileWrite.newLine();
							countTotalRecord++;
						}
						currLine = currFileRead.readLine();
						prevLine = prevFileRead.readLine();
						continue;
					}
				}
				currFileRead.close();
				prevFileRead.close();
				if (countTotalRecord > Integer.parseInt(AppVariables.get(fileName + "_Max_Change_Count").trim())) {
					System.out.println("The number of changes records (" + countTotalRecord + ") written to the output "
							+ "file is greater than the value " + "of the parameter " + fileName + "_Max_Change_Count ("
							+ Integer.parseInt(AppVariables.get(fileName + "_Max_Change_Count").trim())
							+ ") in the parameter Application.props.");
					closeCDSConnection();
					System.exit(THREE);
				}
			} else {
				while ((currLine = currFileRead.readLine()) != null) {
					String currKey = getKeyValue(currLine, fileName);
					String resendKey = fileName + delimiter + currKey;
					if (fileResendMap.containsKey(resendKey)) {
						fileResendMap.put(resendKey, "Y");
					}
					outputFileWrite.write(writeInToFile(currLine));
					outputFileWrite.newLine();
					countTotalRecord++;
				}
				currFileRead.close();
			}
			String totalCount = countTotalRecord + "               ";
			outputFileWrite.write("04" + totalCount.substring(0, 15)
			+ "                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ");
			outputFileWrite.newLine();

			outputFileWrite.close();
		} catch (Exception e) {
			if (currFileRead != null) {
				currFileRead.close();
			}
			if (prevFileRead != null) {
				prevFileRead.close();
			}

			System.out.println(
					"Exception while creating the output file: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(2);
		}
	}

	/**
	 * This method is used to fill the zero in a string.
	 *
	 * @param key key.
	 * @param j   value.
	 * @return string.
	 * @throws Exception exception.
	 */
	public static String zeroFill(final String key, final int j) throws Exception {
		StringBuffer sb = new StringBuffer();
		try {
			for (int i = key.length(); i < j; i++) {
				sb.append("0");
			}
			sb.append(key);
		} catch (Exception e) {
			System.out.println("Exception while filling zero: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(2);
		}
		return sb.toString();
	}

	/**
	 * This method is used to write the record into the current file.
	 *
	 * @param fileName input file name
	 * @throws Exception exception.
	 */
	private static void createCurrFile(final String fileName) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String query = null;
		ResultSetMetaData rsmd = null;
		BufferedWriter buffWrt = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(runtimePath + "Curr_" + fileName + extn)));
		long minReqRecCount = Long.parseLong(AppVariables.get(fileName + "_Min_Required_Record_Count").trim());
		try {
			query = Queries.getQuery(fileName + "_SELECT_QUERY");
			if (query == null) {
				System.out.println(
						"The query " + fileName + "_SELECT_QUERY does not exist " + "in the " + queriesFile + ".");
				buffWrt.close();
				closeCDSConnection();
				System.exit(2);
			}

			String orderBy = "";
			LinkedHashMap<Integer, String> keyColumnsMap = keyOutputMap.get(fileName);
			Iterator<Integer> keyMap = keyColumnsMap.keySet().iterator();
			while (keyMap.hasNext()) {
				int temp = keyMap.next();
				if (!orderBy.equals("")) {
					orderBy = orderBy + ",";
				}
				orderBy = orderBy + keyColumnsMap.get(temp);
			}

			query = query.replace(":1:", schemaName).replace(":2:", clientId).replace(":3:", orderBy)
					.replace(":4:", WorkCountry).replace(":hs:", homeState);
			rs = dbUtil.runSelectQuery(query, false);
			rs.setFetchSize(TENTHOUSAND);
			rsmd = rs.getMetaData();
			int totalRecCount = dbUtil.getRowCount(rs);
			if (totalRecCount < minReqRecCount) {
				System.out.println("The number of records (" + totalRecCount + ") in the current file (" + "Curr_"
						+ fileName + extn + ") is less than the expected record count (" + minReqRecCount
						+ ") set in the parameter " + fileName + "_Min_Required_Record_Count "
						+ " in the Application.props.");
				buffWrt.close();
				closeCDSConnection();
				System.exit(2);
			}
			while (rs.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					if (i != 1) {
						buffWrt.write(delimiter);
					}
					if (rsmd.getColumnTypeName(i).equalsIgnoreCase("DATE")) {
						buffWrt.write(rs.getDate(i) != null ? sdf.format(rs.getDate(i)) : "");
					} else {
						buffWrt.write(
								(rs.getString(i) != null) && (!rs.getString(i).equals("")) ? rs.getString(i).trim()
										: "");
					}
				}
				buffWrt.newLine();
			}

			buffWrt.close();
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			buffWrt.close();
			if (rs != null) {
				rs.close();
			}
			System.out.println(
					"Exception while creating the current file: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(2);
		}
	}

	/**
	 * This method is used to establish the database connection.
	 */
	private static void getCDSConnection() {
		try {
			String dBConn = AppVariables.get("DB_Name").trim();
			dbUtil = new DBUtil(dBConn, dBUname, "", dBPwd);
			dbUtil.getConnection().setAutoCommit(false);
			System.out.println("CDS database connection is established.");
		} catch (Exception e) {
			System.out.println(
					"CDS connection could not be established: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * This method is used to close the database connection.
	 *
	 * @throws Exception exception.
	 */
	private static void closeCDSConnection() throws Exception {
		if (dbUtil != null) {
			dbUtil.getConnection().rollback();
			dbUtil.closeConnection();
			System.out.println("CDS database connection is closed.");
		}
	}

	/**
	 * This method is used to read the records from resend table and store into a
	 * map.
	 *
	 * @param fileName input file name
	 * @throws Exception exception.
	 */
	public static void getResendRecord(final String fileName) throws Exception {
		String lQuery = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer selectQry = new StringBuffer();
		try {
			lQuery = Queries.getQuery("RESEND_QUERY");
			if (lQuery == null) {
				System.out.println("The query RESEND_QUERY does not exist " + "in the " + queriesFile + ".");
				closeCDSConnection();
				System.exit(2);
			}
			LinkedHashMap<Integer, String> keyColumnsMap = keyOutputMap.get(fileName);

			Iterator<Integer> keyMap = keyColumnsMap.keySet().iterator();
			int k = 1;
			while (keyMap.hasNext()) {
				keyMap.next();
				if (selectQry.length() != 0) {
					selectQry.append(",");
				}
				selectQry.append("NVL(KEY_ID" + k + ",'') AS KEYID" + k);
				k++;
			}
			lQuery = lQuery.replace(":1:", schemaName).replace(":2:", clientId).replace(":3:", selectQry.toString())
					.replace(":4:", fileName);
			rs = dbUtil.runSelectQuery(lQuery);
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					String value = rs.getString(i);
					if (value != null) {
						if (sb.length() > 0) {
							sb.append(delimiter);
						}
						sb.append(value.trim());
					}
				}
				fileResendMap.put(fileName + delimiter + sb.toString(), "E");
				sb.setLength(0);
			}
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			if (rs != null) {
				rs.close();
			}
			sb.setLength(0);
			selectQry.setLength(0);
			System.out.println("Exception while reading the records from resend table: " + e + "\n"
					+ Arrays.toString(e.getStackTrace()));
			e.printStackTrace();
			closeCDSConnection();
			System.exit(2);
		}
	}

	/**
	 * This method is used to update the resend table records.
	 *
	 * @param fileName input file name
	 * @throws Exception exception.
	 */
	private static void updateResendTbl(final String fileName) throws Exception {
		StringBuffer sbQry = new StringBuffer();
		String updateQuery;
		try {
			for (Map.Entry<String, String> entry : fileResendMap.entrySet()) {
				String keyVal = entry.getKey();
				updateQuery = Queries.getQuery("UPDATE_RESEND");
				if (updateQuery == null) {
					System.out.println("The query UPDATE_RESEND does not exist " + "in the " + queriesFile + ".");
					closeCDSConnection();
					System.exit(2);
				}
				updateQuery = updateQuery.replace(":1:", String.valueOf(entry.getValue())).replace(":2:", crtUserId)
						.replace(":3:", keyVal.split("\\" + delimiter, -1)[0]).replace(":5:", schemaName)
						.replace(":6:", clientId);

				LinkedHashMap<Integer, String> keyColumnsMap = keyOutputMap.get(fileName);
				Iterator<Integer> keyMap = keyColumnsMap.keySet().iterator();
				int k = 1;
				while (keyMap.hasNext()) {
					keyMap.next();
					sbQry.append(" AND KEY_ID" + k + " = '" + keyVal.split("\\" + delimiter, -1)[k] + "'");
					k++;
				}
				updateQuery = updateQuery.replace(":4:", sbQry.toString());
				sbQry.setLength(0);
				dbUtil.runUpdateQuery(updateQuery, true);
			}
		} catch (Exception e) {
			sbQry.setLength(0);
			System.out.println(
					"Exception while updating the resend table: " + e + "\n" + Arrays.toString(e.getStackTrace()));
			closeCDSConnection();
			System.exit(2);
		}
	}

	private static void loadCountryMap() {
		Locale[] allLocales = Locale.getAvailableLocales();
		for (Locale locale : allLocales) {
			String iso2languages[] = locale.getISOLanguages();
			for (String iso2language : iso2languages) {
				Locale localeTemp = new Locale(iso2language);
				String iso2Countries[] = localeTemp.getISOCountries();
				for (String iso2Country : iso2Countries) {
					Locale locTemp = new Locale(iso2language, iso2Country);
					String iso3Country = locTemp.getISO3Country();
					if (iso3Country.length() == 3) {
						mCountryMap.put(iso2Country, iso3Country);
					}
				}
			}
		}
	}

	/**
	 * @param line Current line to be written into file
	 * @return The line to be written into file
	 * @throws Exception Exception thrown if any.
	 */

	private static String writeInToFile(final String line) throws Exception {
		StringBuffer sb = new StringBuffer();
		String a = line;
		int count = 1;
		try {
			String[] str = line.split("\\" + delimiter, -1);
			for (int i = 0; i < str.length; i++) {
				sb.append(formatString((str[i] == null ? "" : str[i]), fileColsMap.get(count), a));
				count++;
			}
		} catch (Exception e) {
			System.out.println(line);
			System.out.println("Exception in writeInToFile" + e);
			closeCDSConnection();
			System.exit(2);
		}
		return sb.toString();
	}

	/**
	 * Restricts the value or fills with empty space or returns the value as is
	 * based on the size.
	 * 
	 * @param value Value to be converted to the size specified
	 * @param size  size of the value to be returned.
	 * @return Converted String.
	 */
	public static String formatString(final String value, final Integer size, final String a) {
		String resultString = "";

		try {
			if (value.length() > size) {
				resultString = value.substring(0, size);
			} else if (value.length() == size) {
				resultString = value;
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append(value);
				for (int i = value.length(); i < size; i++) {
					sb.append(" ");
				}
				resultString = sb.toString();
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(a);
			System.out.println("Exception in spaceFill " + e);
			System.exit(2);
		}

		return resultString;
	}
}