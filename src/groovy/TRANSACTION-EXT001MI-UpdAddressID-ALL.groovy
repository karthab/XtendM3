/**
 *  Business Engine Extension
 */
/****************************************************************************************
 Extension Name: UpdAddressID
 Type : ExtendM3Transaction
 Script Author: Arun Gopal
 Date: 2025-03-18
 Description: Updating Address ID and Ship-via Address in OIS102/E via API transaction
 Revision History:
 Name                 Date             Version          Description of Changes
 Arun Gopal           2025-03-18       1.0              Initial Version
 ******************************************************************************************/
/**
 * Parameters: (All parameters are mandatory)
 * CONO - Company
 * ORNO - CO number (Mandatory)
 * ADID - Address ID (Mandatory)
 * ADVI - Ship-via address (Mandatory)
 */

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class UpdAddressID extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  String errorMsg;
  String ODADID;
  String ADID;
  String ADVI;
  String CHID;
  int RGDT;
  int RGTM;
  String INRC;
  String DECU;
  String CUNO;
  // Fields of OOADRE
  String CUNM;
  String CUA1;
  String CUA2;
  String CUA3;
  String CUA4;
  String PONO;
  String PHNO;
  String TFNO;
  String YREF;
  String CSCD;
  String VRNO;
  String EDES;
  String ULZO;
  int GEOC;
  String TAXC;
  String ECAR;
  String HAFE;
  String DLSP;
  String DSTX;
  String MODL;
  String TEDL;
  String TEL2;
  String TOWN;
  String FRCO;
  String SPLE;
  String RASN;

  public UpdAddressID(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.utility = utility;
  }

  public void main() {
    int CONO = mi.getIn().get("CONO") == null ? (int) program.getLDAZD().get("CONO") : (int) mi.getIn().get("CONO");
    String ORNO = (String) mi.getIn().get("ORNO");
    ADID = (String) mi.getIn().get("ADID");
    ADVI = (String) mi.getIn().get("ADVI");
    CHID = this.program.getUser();
    RGDT = utility.call("DateUtil", "currentDateY8AsInt");
    RGTM = utility.call("DateUtil", "currentTimeAsInt");

    boolean nullValue = false;
    if (ADID == null && ADVI == null) {
      nullValue = true;
    } else if (ADID != null && ADVI != null) {
      nullValue = true;
    }
    if (nullValue) {
      mi.error("Either Address ID or Ship-via address must be entered.");
      return;
    }
    // Validate Customer order
    boolean recordOOHEAD = validateCustomerOrder(CONO, ORNO);
    if (!recordOOHEAD) {
      mi.error(errorMsg);
      return;
    }
    if (ADID != null && ADVI == null) {
      // Validation for Address ID
      boolean recordODADID = retrieveCurrentAddressID(CONO, ORNO);
      if (!recordODADID) {
        //No action is taken
      } else {
        DBAction dbaOOADRE = database.table("OOADRE")
          .index("00")
          .selection("ODCONO", "ODORNO", "ODADRT", "ODADID")
          .build();
        DBContainer conOOADRE = dbaOOADRE.getContainer();
        conOOADRE.set("ODCONO", CONO);
        conOOADRE.set("ODORNO", ORNO);
        conOOADRE.set("ODADRT", 1);
        conOOADRE.set("ODADID", ODADID);
        if (dbaOOADRE.read(conOOADRE)) {
          String ODADRT = conOOADRE.get("ODADRT");
          //
          boolean returnValue = validateADIDInOCUSAD(CONO, ODADRT);
          if (!returnValue) {
            //No action taken
          } else {
            boolean recordOOADRE = updateShipViaAddressInCOAddress(CONO, ORNO, 1);
            if (!recordOOADRE) {
              //No action taken
            }
          }
        }
      }
    } else if (ADID == null && ADVI != null) {
      // Validation for Ship-via address
      boolean recordODADID = retrieveCurrentAddressID(CONO, ORNO);
      if (!recordODADID) {
        //No action taken
      } else {
        ADID = ODADID;
        //
        DBAction dbaCISHVI00 = database.table("CISHVI")
          .index("00")
          .selection("ILCONO", "ILADVI")
          .build();
        DBContainer conCISHVI00 = dbaCISHVI00.getContainer();
        conCISHVI00.setInt("ILCONO", CONO);
        conCISHVI00.set("ILADVI", ADVI);
        Closure < ? > resultHandlerCISHVI = {
          DBContainer dataCISHVI ->
          String ILADVI = dataCISHVI.get("ILADVI");
          //
          boolean recordOOADRE = updateShipViaAddressInCOAddress(CONO, ORNO, 2);
          if (!recordOOADRE) {
            //No action taken
          }
        }
        if (!dbaCISHVI00.readAll(conCISHVI00, 2, 1, resultHandlerCISHVI)) {
          mi.error("Ship-via address " + ADVI + " is invalid");
          return;
        }
      }
    }
    // Check if Customer order is open
    DBAction dbaOOHEAD = database.table("OOHEAD")
      .index("00")
      .selection("OACONO", "OAORNO", "OAHOCD", "OAJNA", "OAJNU", "OACHID")
      .build();
    DBContainer conOOHEAD = dbaOOHEAD.getContainer();
    conOOHEAD.set("OACONO", CONO);
    conOOHEAD.set("OAORNO", ORNO);
    if (dbaOOHEAD.read(conOOHEAD)) {
      Integer HOCD = conOOHEAD.get("OAHOCD");
      if (HOCD == 1) {
        mi.error("Order " + ORNO + " is locked by job: " + conOOHEAD.getString("OAJNA") + " " + conOOHEAD.getString("OACHID") + " " + conOOHEAD.getInt("OAJNU"));
        return;
      }
    }
    // Update Address ID in CO header
    DBAction queryOOHEAD = database.table("OOHEAD")
      .index("00")
      .selection("OACONO", "OAORNO")
      .build();
    DBContainer containerOOHEAD = queryOOHEAD.getContainer();
    containerOOHEAD.setInt("OACONO", CONO);
    containerOOHEAD.set("OAORNO", ORNO);
    queryOOHEAD.readLock(containerOOHEAD, updateCallBack);
    // Update Address ID in CO lines
    DBAction dbaOOLINE = database.table("OOLINE")
      .index("00")
      .selection("OBCONO", "OBORNO")
      .build();
    DBContainer conOOLINE = dbaOOLINE.getContainer();
    conOOLINE.setInt("OBCONO", CONO);
    conOOLINE.set("OBORNO", ORNO);
    Closure < ? > resultHandlerOOLINE = {
      DBContainer dataOOLINE ->
      Integer PONR = dataOOLINE.get("OBPONR");
      Integer POSX = dataOOLINE.get("OBPOSX");
      //
      DBAction queryOOLINE = database.table("OOLINE")
      .index("00")
      .selection("OBCONO", "OBORNO", "OBPONR", "OBPOSX")
      .build();
      DBContainer containerOOLINE = queryOOLINE.getContainer();
      containerOOLINE.setInt("OBCONO", CONO);
      containerOOLINE.set("OBORNO", ORNO);
      containerOOLINE.setInt("OBPONR", PONR);
      containerOOLINE.setInt("OBPOSX", POSX);
      queryOOLINE.readLock(containerOOLINE, updateCallBack1);
    }
    if (!dbaOOLINE.readAll(conOOLINE, 2, 9999, resultHandlerOOLINE)) {
      //No action taken
    }
    updateShipViaAddressInCustomerAddress(CONO, CUNO);
    updateADVI_ADIDInDelivery(CONO, ORNO);
  }

  private String updateADVI_ADIDInDelivery(int CONO, String ORNO) {
    // Update Address ID and Ship-via Address in Delivery
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
      long DLIX = dataMHDISH.get("OQDLIX");
      //
      DBAction queryMHDISH = database.table("MHDISH")
      .index("00")
      .selection("OQCONO", "OQINOU", "OQDLIX")
      .build();
      DBContainer containerMHDISH = queryMHDISH.getContainer();
      containerMHDISH.setInt("OQCONO", CONO);
      containerMHDISH.setInt("OQINOU", 1);
      containerMHDISH.setLong("OQDLIX", DLIX);
      queryMHDISH.readLock(containerMHDISH, updateCallBack4);
    }
    if (!dbaMHDISH.readAll(conMHDISH, 2, 9999, resultHandlerMHDISH)) {
      //No action taken
    }
  }

  private String updateShipViaAddressInCustomerAddress(int CONO, String CUNO) {
    DBAction queryOCUSAD = database.table("OCUSAD")
      .index("00")
      .selection("OPCONO", "OPCUNO", "OPADRT", "OPADID")
      .build();
    DBContainer containerOCUSAD = queryOCUSAD.getContainer();
    containerOCUSAD.setInt("OPCONO", CONO);
    containerOCUSAD.set("OPCUNO", CUNO);
    containerOCUSAD.setInt("OPADRT", 1);
    containerOCUSAD.set("OPADID", ODADID);
    queryOCUSAD.readLock(containerOCUSAD, updateCallBack3);
  }

  private boolean retrieveCurrentAddressID(int CONO, String ORNO) {
    boolean value = "";
    DBAction dbOOADRE = database.table("OOADRE")
      .index("00")
      .selection("ODCONO", "ODORNO", "ODADRT", "ODADID")
      .build();
    DBContainer contOOADRE = dbOOADRE.getContainer();
    contOOADRE.setInt("ODCONO", CONO);
    contOOADRE.set("ODORNO", ORNO);
    contOOADRE.setInt("ODADRT", 1);
    Closure < ? > resultHandlerOOADRE = {
      DBContainer dataOOADRE ->
      ODADID = dataOOADRE.get("ODADID");
    }
    if (!dbOOADRE.readAll(contOOADRE, 3, 1, resultHandlerOOADRE)) {
      errorMsg = "Record does not exist";
      value = false;
    } else {
      errorMsg = "";
      value = true;
    }
    return value;
  }

  private boolean validateCustomerOrder(int CONO, String ORNO) {
    boolean value = true;
    DBAction dbaOOHEAD00 = database.table("OOHEAD")
      .index("00")
      .selection("OACONO", "OAORNO", "OAINRC", "OADECU", "OACUNO")
      .build();
    DBContainer conOOHEAD00 = dbaOOHEAD00.getContainer();
    conOOHEAD00.set("OACONO", CONO);
    conOOHEAD00.set("OAORNO", ORNO);
    if (!dbaOOHEAD00.read(conOOHEAD00)) {
      errorMsg = "Order number " + ORNO + " does not exist in the Company " + CONO;
      value = false;
    } else {
      INRC = conOOHEAD00.get("OAINRC");
      DECU = conOOHEAD00.get("OADECU");
      CUNO = conOOHEAD00.get("OACUNO");
      errorMsg = "";
      value = true;
    }
    return value;
  }

  private boolean validateADIDInOCUSAD(int CONO, String ODADRT) {
    boolean value = true;
    DBAction dbaOCUSAD = database.table("OCUSAD")
      .index("00")
      .selection("OPCONO", "OPCUNO", "OPADRT", "OPADVI", "OPADID", "OPCUNM", "OPCUNM", "OPCUA1",
        "OPCUNM", "OPCUA1", "OPCUA2", "OPCUA3", "OPCUA4", "OPPONO", "OPPHNO", "OPTFNO",
        "OPYREF", "OPCSCD", "OPVRNO", "OPEDES", "OPULZO", "OPGEOC", "OPTAXC", "OPECAR",
        "OPHAFE", "OPMODL", "OPTEDL", "OPTEL2", "OPTOWN", "OPFRCO", "OPSPLE", "OPRASN")
      .build();
    DBContainer conOCUSAD = dbaOCUSAD.getContainer();
    conOCUSAD.set("OPCONO", CONO);
    if (ODADRT.equals("3")) {
      conOCUSAD.set("OPCUNO", INRC);
    } else if (ODADRT.equals("1")) {
      conOCUSAD.set("OPCUNO", DECU);
    } else {
      conOCUSAD.set("OPCUNO", CUNO);
    }
    conOCUSAD.set("OPADRT", 1);
    conOCUSAD.set("OPADID", ADID);
    if (!dbaOCUSAD.read(conOCUSAD)) {
      errorMsg = "Address ID " + ADID + " is invalid";
      value = false;
    } else {
      ADVI = conOCUSAD.getString("OPADVI");
      CUNM = conOCUSAD.getString("OPCUNM");
      CUA1 = conOCUSAD.getString("OPCUA1");
      CUA2 = conOCUSAD.getString("OPCUA2");
      CUA3 = conOCUSAD.getString("OPCUA3");
      CUA4 = conOCUSAD.getString("OPCUA4");
      PONO = conOCUSAD.getString("OPPONO");
      PHNO = conOCUSAD.getString("OPPHNO");
      TFNO = conOCUSAD.getString("OPTFNO");
      YREF = conOCUSAD.getString("OPYREF");
      CSCD = conOCUSAD.getString("OPCSCD");
      VRNO = conOCUSAD.getString("OPVRNO");
      EDES = conOCUSAD.getString("OPEDES");
      ULZO = conOCUSAD.getString("OPULZO");
      GEOC = conOCUSAD.get("OPGEOC");
      TAXC = conOCUSAD.getString("OPTAXC");
      ECAR = conOCUSAD.getString("OPECAR");
      HAFE = conOCUSAD.getString("OPHAFE");
      MODL = conOCUSAD.getString("OPMODL");
      TEDL = conOCUSAD.getString("OPTEDL");
      TEL2 = conOCUSAD.getString("OPTEL2");
      TOWN = conOCUSAD.getString("OPTOWN");
      FRCO = conOCUSAD.getString("OPFRCO");
      SPLE = conOCUSAD.getString("OPSPLE");
      RASN = conOCUSAD.getString("OPRASN");
      errorMsg = "";
      value = true;
    }
    return value;
  }

  private boolean updateShipViaAddressInCOAddress(int CONO, String ORNO, int condition) {
    boolean value = true;
    if (condition == 1) {
      DBAction dbaOOADRE00 = database.table("OOADRE")
        .index("00")
        .selection("ODCONO", "ODORNO", "ODADRT", "ODADID")
        .build();
      DBContainer conOOADRE00 = dbaOOADRE00.getContainer();
      conOOADRE00.setInt("ODCONO", CONO);
      conOOADRE00.set("ODORNO", ORNO);
      conOOADRE00.setInt("ODADRT", 1);
      conOOADRE00.set("ODADID", ODADID);
      // Delete record from the table OOADRE
      Closure < ? > deleteOOADRECallback = {
        LockedResult lockedResult ->
        lockedResult.delete();
      }
      if (!dbaOOADRE00.readLock(conOOADRE00, deleteOOADRECallback)) {
        errorMsg = "Record does not exist";
        value = false;
      } else {
        errorMsg = "";
        value = true;
      }
      if (value) {
        // Update Address ID in CO connect address
        DBAction queryOOADRE = database.table("OOADRE")
          .index("00")
          .selection("ODCONO", "ODORNO", "ODADRT", "ODADID")
          .build();
        DBContainer containerOOADRE = queryOOADRE.createContainer();
        containerOOADRE.set("ODCONO", CONO);
        containerOOADRE.set("ODORNO", ORNO);
        containerOOADRE.set("ODADRT", 1);
        containerOOADRE.set("ODADID", ADID);
        containerOOADRE.set("ODCUNM", CUNM);
        containerOOADRE.set("ODCUA1", CUA1);
        containerOOADRE.set("ODCUA2", CUA2);
        containerOOADRE.set("ODCUA3", CUA3);
        containerOOADRE.set("ODCUA4", CUA4);
        containerOOADRE.set("ODPONO", PONO);
        containerOOADRE.set("ODPHNO", PHNO);
        containerOOADRE.set("ODTFNO", TFNO);
        containerOOADRE.set("ODYREF", YREF);
        containerOOADRE.set("ODCSCD", CSCD);
        containerOOADRE.set("ODVRNO", VRNO);
        containerOOADRE.set("ODEDES", EDES);
        containerOOADRE.set("ODULZO", ULZO);
        containerOOADRE.set("ODGEOC", GEOC);
        containerOOADRE.set("ODTAXC", TAXC);
        containerOOADRE.set("ODECAR", ECAR);
        containerOOADRE.set("ODHAFE", HAFE);
        containerOOADRE.set("ODDLSP", DLSP);
        containerOOADRE.set("ODDSTX", DSTX);
        containerOOADRE.set("ODMODL", MODL);
        containerOOADRE.set("ODTEDL", TEDL);
        containerOOADRE.set("ODTEL2", TEL2);
        containerOOADRE.set("ODTOWN", TOWN);
        containerOOADRE.set("ODRGDT", RGDT);
        containerOOADRE.set("ODRGTM", RGTM);
        containerOOADRE.set("ODLMDT", RGDT);
        containerOOADRE.set("ODCHNO", 1);
        containerOOADRE.set("ODCHID", CHID);
        containerOOADRE.set("ODADVI", ADVI);
        containerOOADRE.set("ODFRCO", FRCO);
        containerOOADRE.set("ODSPLE", SPLE);
        containerOOADRE.set("ODRASN", RASN);
        boolean result = queryOOADRE.insert(containerOOADRE);
        if (!result) {
          errorMsg = "Error while adding record to table OOADRE";
          value = false;
        } else {
          errorMsg = "";
          value = true;
        }
      }
    } else if (condition == 2) {
      DBAction queryOOADRE = database.table("OOADRE")
        .index("00")
        .selection("ODCONO", "ODORNO", "ODADRT", "ODADID")
        .build();
      DBContainer containerOOADRE = queryOOADRE.getContainer();
      containerOOADRE.setInt("ODCONO", CONO);
      containerOOADRE.set("ODORNO", ORNO);
      containerOOADRE.setInt("ODADRT", 1);
      containerOOADRE.set("ODADID", ODADID);
      queryOOADRE.readLock(containerOOADRE, updateCallBack2);
    }
    return value;
  }

  Closure < ? > updateCallBack4 = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("OQCHNO") + 1
    lockedResult.set("OQCOAA", ADID);
    lockedResult.set("OQCOAF", ADID);
    lockedResult.set("OQADVI", ADVI);
    lockedResult.set("OQLMDT", LMDT);
    lockedResult.set("OQCHNO", CHNO);
    lockedResult.set("OQCHID", CHID);
    lockedResult.update();
  }

  Closure < ? > updateCallBack3 = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("OPCHNO") + 1
    lockedResult.set("OPADVI", ADVI);
    lockedResult.set("OPLMDT", LMDT);
    lockedResult.set("OPCHNO", CHNO);
    lockedResult.set("OPCHID", CHID);
    lockedResult.update();
  }

  Closure < ? > updateCallBack2 = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("ODCHNO") + 1
    lockedResult.set("ODADVI", ADVI);
    lockedResult.set("ODLMDT", LMDT);
    lockedResult.set("ODCHNO", CHNO);
    lockedResult.set("ODCHID", CHID);
    lockedResult.update();
  }

  Closure < ? > updateCallBack1 = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("OBCHNO") + 1
    lockedResult.set("OBADID", ADID);
    lockedResult.set("OBLMDT", LMDT);
    lockedResult.set("OBCHNO", CHNO);
    lockedResult.set("OBCHID", CHID);
    lockedResult.update();
  }

  Closure < ? > updateCallBack = {
    LockedResult lockedResult ->
    int LMDT = RGDT;
    int CHNO = (Integer) lockedResult.get("OACHNO") + 1
    lockedResult.set("OAADID", ADID);
    lockedResult.set("OALMDT", LMDT);
    lockedResult.set("OACHNO", CHNO);
    lockedResult.set("OACHID", CHID);
    lockedResult.update();
  }
}