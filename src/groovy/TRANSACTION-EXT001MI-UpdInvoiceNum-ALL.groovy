/**
 *  Business Engine Extension
 */
/****************************************************************************************
 Extension Name: UpdInvoiceNum
 Type : ExtendM3Transaction
 Script Author: Arun Gopal
 Date: 2025-04-09
 Description: Update Invoice number to the corresponding CO number
 Revision History:
 Name                 Date             Version          Description of Changes
 Arun Gopal           2025-04-09       1.0              Initial Version
 ******************************************************************************************/
/**
 * Parameters: (All parameters are mandatory)
 * CONO - Company
 * DIVI - Division (Mandatory)
 * EXIN - Extended Invoice number (Mandatory)
 */

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class UpdInvoiceNum extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  String CHID;
  String EXIN;
  int RGDT;
  int RGTM;

  public UpdInvoiceNum(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.utility = utility;
  }

  public void main() {
    int CONO = mi.getIn().get("CONO") == null ? (int) program.getLDAZD().get("CONO") : (int) mi.getIn().get("CONO");
    String DIVI = (String) mi.getIn().get("DIVI");
    EXIN = (String) mi.getIn().get("EXIN");
    CHID = this.program.getUser();
    RGDT = utility.call("DateUtil", "currentDateY8AsInt");
    RGTM = utility.call("DateUtil", "currentTimeAsInt");

    if (DIVI == null) {
      mi.error("Division should be entered");
      return;
    }
    if (EXIN == null) {
      mi.error("Extended Invoice number should be entered");
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
    //Validation for Invoice number
    int YEA4 = 0;
    String INPX = "";
    int IVNO = 0;
    ExpressionFactory expression = database.getExpressionFactory("OINVOH");
    expression = expression.eq("UHEXIN", EXIN);
    DBAction dbaOINVOH = database.table("OINVOH")
      .index("00")
      .matching(expression)
      .selection("UHCONO", "UHDIVI", "UHEXIN", "UHYEA4", "UHINPX", "UHIVNO")
      .build();
    DBContainer conOINVOH = dbaOINVOH.getContainer();
    conOINVOH.setInt("UHCONO", CONO);
    conOINVOH.set("UHDIVI", DIVI);
    Closure < ? > resultHandlerOINVOH = {
      DBContainer dataOINVOH ->
      YEA4 = dataOINVOH.get("UHYEA4");
      INPX = dataOINVOH.get("UHINPX");
      IVNO = dataOINVOH.get("UHIVNO");
    }
    if (!dbaOINVOH.readAll(conOINVOH, 2, 1, resultHandlerOINVOH)) {
      mi.error("Invoice number " + EXIN + " is invalid");
      return;
    }
    //Fetch customer order number
    String ORNO = "";
    long DLIX;
    DBAction dbaODLINE = database.table("ODLINE")
      .index("20")
      .selection("UBCONO", "UBDIVI", "UBYEA4", "UBINPX", "UBIVNO", "UBORNO", "UBDLIX")
      .build();
    DBContainer conODLINE = dbaODLINE.getContainer();
    conODLINE.setInt("UBCONO", CONO);
    conODLINE.set("UBDIVI", DIVI);
    conODLINE.setInt("UBYEA4", YEA4);
    conODLINE.set("UBINPX", INPX);
    conODLINE.setInt("UBIVNO", IVNO);
    Closure < ? > resultHandlerODLINE = {
      DBContainer dataODLINE ->
      ORNO = dataODLINE.get("UBORNO");
      DLIX = dataODLINE.get("UBDLIX");
    }
    if (!dbaODLINE.readAll(conODLINE, 5, 1, resultHandlerODLINE)) {
      mi.error("Record does not exist");
      return;
    }
    //Update Invoice number to extension table
    DBAction queryEXTINV = database.table("EXTINV")
      .index("00")
      .selection("EXCONO", "EXDIVI", "EXORNO", "EXDLIX")
      .build();
    DBContainer containerEXTINV = queryEXTINV.getContainer();
    containerEXTINV.set("EXCONO", CONO);
    containerEXTINV.set("EXDIVI", DIVI);
    containerEXTINV.set("EXORNO", ORNO);
    containerEXTINV.set("EXDLIX", DLIX);
    queryEXTINV.readLock(containerEXTINV, updateCallBack);
  }

  Closure < ? > updateCallBack = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("EXCHNO") + 1
    lockedResult.set("EXUDF0", EXIN);
    lockedResult.set("EXLMDT", LMDT);
    lockedResult.set("EXCHNO", CHNO);
    lockedResult.set("EXCHID", CHID);
    lockedResult.update();
  }
}