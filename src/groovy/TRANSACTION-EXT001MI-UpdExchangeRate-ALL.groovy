/**
 *  Business Engine Extension
 */
/****************************************************************************************
 Extension Name: UpdExchangeRate
 Type : ExtendM3Transaction
 Script Author: Arun Gopal
 Date: 2025-04-03
 Description: Update recent exchange rate from CRS055 w.r.t. Invoice date
 Revision History:
 Name                 Date             Version          Description of Changes
 Arun Gopal           2025-04-03       1.0              Initial Version
 Arun Gopal           2025-06-13       1.1              Adding method comments
 ******************************************************************************************/
/**
 * Parameters: (All parameters are mandatory)
 * CONO - Company
 * DIVI - Division (Mandatory)
 * ORNO - Customer order number
 */

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class UpdExchangeRate extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  String CHID;
  String ARAT;
  int RGDT;
  int RGTM;

  public UpdExchangeRate(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.utility = utility;
  }

  /**
   * Main method
   * @return
   */
  public void main() {
    int CONO = mi.getIn().get("CONO") == null ? (int) program.getLDAZD().get("CONO") : (int) mi.getIn().get("CONO");
    String DIVI = (String) mi.getIn().get("DIVI");
    String ORNO = (String) mi.getIn().get("ORNO");
    CHID = this.program.getUser();
    RGDT = utility.call("DateUtil", "currentDateY8AsInt");
    RGTM = utility.call("DateUtil", "currentTimeAsInt");
    int IVDT = RGDT;
    long DLIX;

    if (DIVI == null) {
      mi.error("Division should be entered");
      return;
    }
    //Validation for Division
    DBAction dbaCMNDIV = database.table("CMNDIV").index("00").build();
    DBContainer conCMNDIV = dbaCMNDIV.getContainer();
    conCMNDIV.set("CCCONO", CONO);
    conCMNDIV.set("CCDIVI", DIVI);
    if (!dbaCMNDIV.read(conCMNDIV)) {
      mi.error("Division " + DIVI + " does not exist");
      return;
    }
    //Fetch exchange rates
    String ARAT_GBP = fetchExchangeRate(CONO, DIVI, "GBP", IVDT);
    String ARAT_USD = fetchExchangeRate(CONO, DIVI, "USD", IVDT);
    String ARAT_EUR = fetchExchangeRate(CONO, DIVI, "EUR", IVDT);
    //Check record in OIS151
    DBAction dbaOOI150 = database.table("OOI150")
      .index("20")
      .selection("HHORNO", "HHDLIX")
      .build();
    DBContainer conOOI150 = dbaOOI150.getContainer();
    Closure < ? > resultHandlerOOI150 = {
      DBContainer dataOOI150 ->
      ORNO = dataOOI150.get("HHORNO");
      DLIX = dataOOI150.get("HHDLIX");
      // Insert record in Custom table
      insertRecordInCustomTable(CONO, DIVI, ORNO, DLIX, ARAT_GBP, ARAT_USD, ARAT_EUR);
    }
    if (!dbaOOI150.readAll(conOOI150, 0, 9999, resultHandlerOOI150)) {
      //Validate CO number
      DBAction dbaOOHEAD = database.table("OOHEAD").index("00").build();
      DBContainer conOOHEAD = dbaOOHEAD.getContainer();
      conOOHEAD.set("OACONO", CONO);
      conOOHEAD.set("OAORNO", ORNO);
      if (!dbaOOHEAD.read(conOOHEAD)) {
        ORNO = "";
      } else {
        //Fetch delivery number
        DLIX = fetchDeliveryNumber(CONO, ORNO);
        insertRecordInCustomTable(CONO, DIVI, ORNO, DLIX, ARAT_GBP, ARAT_USD, ARAT_EUR);
      }
      return;
    }
  }

  /**
   * Write record to table EXTINV
   */
  private String insertRecordInCustomTable(int CONO, String DIVI, String ORNO, long DLIX, String GBP, String USD, String EUR) {
    deleteRecordFromEXTINV(CONO, DIVI, ORNO, DLIX);
    DBAction queryEXTINV = database.table("EXTINV")
      .index("00")
      .selection("EXCONO", "EXDIVI", "EXORNO", "EXDLIX")
      .build();
    DBContainer containerEXTINV = queryEXTINV.createContainer();
    containerEXTINV.set("EXCONO", CONO);
    containerEXTINV.set("EXDIVI", DIVI);
    containerEXTINV.set("EXORNO", ORNO);
    containerEXTINV.set("EXDLIX", DLIX);
    containerEXTINV.set("EXUDF1", GBP);
    containerEXTINV.set("EXUDF2", USD);
    containerEXTINV.set("EXUDF3", EUR);
    containerEXTINV.set("EXRGDT", RGDT);
    containerEXTINV.set("EXRGTM", RGTM);
    containerEXTINV.set("EXLMDT", RGDT);
    containerEXTINV.set("EXCHNO", 1);
    containerEXTINV.set("EXCHID", CHID);
    queryEXTINV.insert(containerEXTINV);
  }

  /**
   * Retrieve the Delivery number
   * @return Delivery number
   */
  private long fetchDeliveryNumber(int CONO, String ORNO) {
    long value = 0;
    ExpressionFactory expression = database.getExpressionFactory("MHDISH");
    expression = expression.eq("OQRIDN", ORNO);
    DBAction dbaMHDISH = database.table("MHDISH")
      .index("00")
      .matching(expression)
      .selection("OQCONO", "OQINOU", "OQRIDN", "OQDLIX")
      .build();
    DBContainer conMHDISH = dbaMHDISH.getContainer();
    conMHDISH.setInt("OQCONO", CONO);
    conMHDISH.setInt("OQINOU", 1);
    Closure < ? > resultHandlerMHDISH = {
      DBContainer dataMHDISH ->
      value = dataMHDISH.get("OQDLIX");
    }
    if (!dbaMHDISH.readAll(conMHDISH, 2, 1, resultHandlerMHDISH)) {
      //No action taken
      return;
    }
    return value;
  }

  /**
   * Delete record from table EXTINV
   */
  private String deleteRecordFromEXTINV(int CONO, String DIVI, String ORNO, long DLIX) {
    DBAction dbaEXTINV = database.table("EXTINV")
      .index("00")
      .selection("EXCONO", "EXDIVI", "EXORNO", "EXDLIX")
      .build();
    DBContainer conEXTINV = dbaEXTINV.getContainer();
    conEXTINV.setInt("EXCONO", CONO);
    conEXTINV.set("EXDIVI", DIVI);
    conEXTINV.set("EXORNO", ORNO);
    conEXTINV.setLong("EXDLIX", DLIX);
    // Delete record from the table OOADRE
    Closure < ? > deleteEXTINVCallback = {
      LockedResult lockedResult ->
      lockedResult.delete();
    }
    if (!dbaEXTINV.readLock(conEXTINV, deleteEXTINVCallback)) {
      //Next step
    }
  }

  /**
   * Retrieve the Current exchange rate based on the Invoice date
   * @return Current exchange rate
   */
  private String fetchExchangeRate(int CONO, String DIVI, String CUCD, int IVDT) {
    String value = "";
    ExpressionFactory expression = database.getExpressionFactory("CCURRA");
    expression = expression.le("CUCUTD", String.valueOf(IVDT));
    DBAction dbaCCURRA = database.table("CCURRA")
      .index("00")
      .matching(expression)
      .selection("CUCONO", "CUDIVI", "CUCUCD", "CUCRTP", "CUCUTD", "CUARAT")
      .reverse()
      .build();
    DBContainer conCCURRA = dbaCCURRA.getContainer();
    conCCURRA.setInt("CUCONO", CONO);
    conCCURRA.set("CUDIVI", DIVI);
    conCCURRA.set("CUCUCD", CUCD);
    conCCURRA.setInt("CUCRTP", 1);
    Closure < ? > resultHandlerCCURRA = {
      DBContainer dataCCURRA ->
      value = dataCCURRA.get("CUARAT");
    }
    if (!dbaCCURRA.readAll(conCCURRA, 4, 1, resultHandlerCCURRA)) {
      mi.error("Currency " + CUCD + " is invalid");
      return;
    }
    return value;
  }
}