package com.learning.exceltojson;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelToJson {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Map<String,List<String>> map=new ExcelToJson().getExcelData();
		System.out.println(map);
		
	}
	public  Map<String,List<String>> getExcelData(){
		Map<String,List<String>> excelDataMap=new HashMap<String, List<String>>();
		try {

			FileInputStream file = new FileInputStream(new File("E:\\SIVA'S TASK\\all_QA1_test_data_excludebanners.xlsx"));
			Workbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			Sheet sheet = workbook.getSheetAt(0);

			Row row=sheet.getRow(0);
			for(Cell cell :row) {
				excelDataMap.put(cell.getStringCellValue(),getColumnsValues(sheet,cell.getColumnIndex()));
			}

			workbook.close();
			file.close();

		}
		catch (Exception e) {
			// TODO: handle exception
		}



		return excelDataMap;
	}

	private List<String> getColumnsValues(Sheet sheet,int columnNumber){
		List<String> values=new ArrayList<String>();

		for(Row row:sheet) {
			if(row.getRowNum() !=0) {
				Cell cell =row.getCell(columnNumber);
				if(cell.getCellType().equals(CellType.STRING)) {
					values.add(cell.getStringCellValue());
				}else if(cell.getCellType().equals(CellType.NUMERIC)) {
					values.add(String.valueOf(cell.getNumericCellValue()));
				}	
			}
		}
		return values;
	}


}
