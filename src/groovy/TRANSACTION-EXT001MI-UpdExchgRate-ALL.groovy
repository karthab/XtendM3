/**
 *  Business Engine Extension
 */
/****************************************************************************************
 Extension Name: UpdExchgRate
 Type : ExtendM3Transaction
 Script Author: Arun Gopal
 Date: 2025-04-15
 Description: Update recent exchange rate from CRS055 w.r.t. Invoice date
 Revision History:
 Name                 Date             Version          Description of Changes
 Arun Gopal           2025-04-15       1.0              Initial Version
 Arun Gopal           2025-06-13       1.1              Adding method comments
 ******************************************************************************************/
/**
 * Parameters: (All parameters are mandatory)
 * CONO - Company
 * DIVI - Division (Mandatory)
 * ORNO - Customer order number (Mandatory)
 * DLIX - Delivery number (Mandatory)
 */

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class UpdExchgRate extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  String CHID;
  String ARAT;
  int RGDT;
  int RGTM;

  public UpdExchgRate(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
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
    long DLIX = (long) mi.getIn().get("DLIX");
    CHID = this.program.getUser();
    RGDT = utility.call("DateUtil", "currentDateY8AsInt");
    RGTM = utility.call("DateUtil", "currentTimeAsInt");
    int IVDT = RGDT;

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
    //Validate CO number
    DBAction dbaOOHEAD = database.table("OOHEAD").index("00").build();
    DBContainer conOOHEAD = dbaOOHEAD.getContainer();
    conOOHEAD.set("OACONO", CONO);
    conOOHEAD.set("OAORNO", ORNO);
    if (!dbaOOHEAD.read(conOOHEAD)) {
      mi.error("Customer order does not exist");
      return;
    } else {
      insertRecordInCustomTable(CONO, DIVI, ORNO, DLIX, ARAT_GBP, ARAT_USD, ARAT_EUR);
    }
  }

  /**
   * Write record to table EXTINV
   */
  private String insertRecordInCustomTable(int CONO, String DIVI, String ORNO, long DLIX, String GBP, String USD, String EUR) {
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