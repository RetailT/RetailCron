const mssql = require("mssql");
const axios = require("axios");
require("dotenv").config();
const qs = require("qs");
const https = require("https");


const dbConfig1 = {
  user: process.env.DB_USER, // Database username
  password: process.env.DB_PASSWORD, // Database password
  server: process.env.DB_SERVER, // Database server address
  database: process.env.DB_DATABASE1, // Database name
  options: {
    encrypt: false, // Disable encryption
    trustServerCertificate: true, // Trust server certificate (useful for local databases)
  },
  port: 1443, // Default MSSQL port (1433)
};

const logs = [];
function addLog(status, message, context = {}) {
  const entry = {
    ts: new Date().toISOString(),
    status, // "SUCCESS" | "ERROR" | "INFO" | "WARN"
    message,
    context,
  };
  logs.push(entry);

  // mirror to console
  const fn =
    status === "ERROR"
      ? console.error
      : status === "WARN"
      ? console.warn
      : console.log;
  fn(`${status}: ${message}`, Object.keys(context).length ? context : "");
  return entry;
}

async function syncDBConnection() {
  try {
    const request = new mssql.Request();
    const query = "SELECT * FROM tb_SYNCDB_USERS";
    const result = await request.query(query);

    if (result.recordset.length === 0) {
      addLog("WARN", "No customer data found in tb_SYNCDB_USERS");
      return [];
    }

    addLog("SUCCESS", "Fetched sync DB connection data", {
      count: result.recordset.length,
    });
    return result.recordset;
  } catch (error) {
    addLog("ERROR", "Error fetching sync DB connection data", {
      error: error.message,
    });
    return [];
  }
}

async function userItemsDetails(ReceiptDate, ReceiptNo) {
  try {
    const request = new mssql.Request();

    const result = await request.query`
      SELECT Item_Desc, ItemAmt, ItemDiscountAmt 
      FROM tb_OGFITEMSALE 
      WHERE ReceiptDate = ${ReceiptDate} 
        AND ReceiptNo = ${ReceiptNo} 
        AND UPLOAD <> 'T'
    `;

    if (result.recordset.length === 0) {
      const msg = "No user items details found for given receipt";
      addLog("WARN", msg, { ReceiptDate, ReceiptNo });
      return { error: msg };
    }

    addLog("SUCCESS", "Fetched user items details", {
      ReceiptDate,
      ReceiptNo,
      count: result.recordset.length,
    });
    return result.recordset;
  } catch (error) {
    const msg = `Error fetching user items details: ${error.message}`;
    addLog("ERROR", msg, { ReceiptDate, ReceiptNo });
    return { error: msg };
  }
}

async function userPaymentDetails() {
  try {
    const request = new mssql.Request();

    const result = await request.query`
      SELECT 
        ReceiptNo, 
        MAX(ReceiptDate) AS ReceiptDate, 
        MAX(ReceiptTime) AS ReceiptTime, 
        SUM(NoOfItems) AS NoOfItems, 
        MAX(SalesCurrency) AS SalesCurrency, 
        SUM(TotalSalesAmtB4Tax) AS TotalSalesAmtB4Tax, 
        SUM(TotalSalesAmtAfterTax) AS TotalSalesAmtAfterTax, 
        SUM(SalesTaxRate) AS SalesTaxRate, 
        SUM(ServiceChargeAmt) AS ServiceChargeAmt, 
        SUM(PaymentAmt) AS PaymentAmt, 
        MAX(PaymentCurrency) AS PaymentCurrency, 
        (SELECT STUFF(
            (SELECT DISTINCT ',' + t2.PaymentMethod  
             FROM tb_OGFPAYMENT AS t2  
             WHERE t2.ReceiptNo = t1.ReceiptNo  
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
        ) AS PaymentMethod, 
        MAX(SalesType) AS SalesType
      FROM tb_OGFPAYMENT AS t1 
      WHERE UPLOAD <> 'T'
      GROUP BY ReceiptNo;
    `;

    if (result.recordset.length === 0) {
      const msg = "Cannot fetch user payment details (0 rows)";
      addLog("WARN", msg);
      return { error: msg };
    }

    addLog("SUCCESS", "Fetched user payment details", {
      count: result.recordset.length,
    });
    return result.recordset;
  } catch (error) {
    const msg = `Error fetching user payment details: ${error.message}`;
    addLog("ERROR", msg);
    return { error: msg };
  }
}

async function userDetails() {
  try {
    const request = new mssql.Request();

    const result = await request.query`
      SELECT 
        AppCode, PropertyCode, POSInterfaceCode, BatchCode, SalesTaxRate, OAUTH_TOKEN_URL, 
        ClientID, ClientSecret, API_ENDPOINT  
      FROM tb_OGFMAIN;
    `;

    if (result.recordset.length === 0) {
      const msg = "Cannot fetch user details (0 rows)";
      addLog("WARN", msg);
      return [];
    }

    const trimmed = result.recordset.map((user) => {
      const trimmedUser = {};
      for (const key in user) {
        trimmedUser[key] =
          typeof user[key] === "string" ? user[key].trim() : user[key];
      }
      return trimmedUser;
    });

    addLog("SUCCESS", "Fetched user details", { count: trimmed.length });
    return trimmed;
  } catch (error) {
    addLog("ERROR", "Error fetching user connection details", {
      error: error.message,
    });
    return [];
  }
}

async function getAccessToken(user) {
  try {
    const data = qs.stringify({
      client_id: user.ClientID,
      client_secret: user.ClientSecret,
      grant_type: "client_credentials",
    });

    const agent = new https.Agent({ family: 4 }); // Force IPv4

    const response = await axios.post(user.OAUTH_TOKEN_URL, data, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      httpsAgent: agent,
      timeout: 10000,
    });

    addLog("SUCCESS", "Access token fetched", { AppCode: user.AppCode });
    return response.data.access_token;
  } catch (error) {
    addLog("ERROR", "Error fetching token", {
      AppCode: user.AppCode,
      error: error.response ? error.response.data : error.message,
    });
    return null;
  }
}


function trimObjectStrings(obj) {
  if (obj === null || typeof obj !== "object") return obj;
  if (Array.isArray(obj)) return obj.map(trimObjectStrings);

  return Object.fromEntries(
    Object.entries(obj).map(([key, value]) => [
      key,
      typeof value === "string" ? value.trim() : trimObjectStrings(value),
    ])
  );
}

async function updateTables() {
  const transaction = new mssql.Transaction();
  try {
    await transaction.begin();

    const request = new mssql.Request(transaction);

    const updatePayment = await request.query(`
      UPDATE tb_OGFPAYMENT
      SET UPLOAD = 'T'
      WHERE UPLOAD <> 'T' OR UPLOAD IS NULL;
    `);

    const updateItems = await request.query(`
      UPDATE tb_OGFITEMSALE
      SET UPLOAD = 'T'
      WHERE UPLOAD <> 'T' OR UPLOAD IS NULL;
    `);

    await transaction.commit();

    const paymentRows = updatePayment.rowsAffected[0];
    const itemsRows = updateItems.rowsAffected[0];

    if (paymentRows === 0 && itemsRows === 0) {
      const res = {
        status: "ERROR",
        message:
          "No rows were updated in tb_OGFPAYMENT or tb_OGFITEMSALE",
        paymentRowsAffected: paymentRows,
        itemsRowsAffected: itemsRows,
      };
      addLog("WARN", res.message, {
        paymentRowsAffected: paymentRows,
        itemsRowsAffected: itemsRows,
      });
      return res;
    }

    const res = {
      status: "SUCCESS",
      message: "Tables updated successfully",
      paymentRowsAffected: paymentRows,
      itemsRowsAffected: itemsRows,
    };
    addLog("SUCCESS", res.message, {
      paymentRowsAffected: paymentRows,
      itemsRowsAffected: itemsRows,
    });
    return res;
  } catch (error) {
    await transaction.rollback();
    const res = {
      status: "ERROR",
      message: "Could not update tables",
      paymentRowsAffected: 0,
      itemsRowsAffected: 0,
    };
    addLog("ERROR", res.message, { error: error.message });
    return res;
  }
}

async function syncDB() {
  addLog("INFO", "Starting syncDB");
  try {
    await mssql.close();
    addLog("INFO", "Connecting to primary DB", { server: dbConfig1.server });
    await mssql.connect(dbConfig1);
    addLog("SUCCESS", "Connected to primary DB", { server: dbConfig1.server });

    const dbConnectionData = await syncDBConnection();

    if (!dbConnectionData || dbConnectionData.length === 0) {
      const msg = "No customer data found.";
      addLog("WARN", msg);
      return { responses: [], errors: [msg], logs };
    }

    const apiResponses = [];
    const errors = [];

    for (const customer of dbConnectionData) {
      const syncdbIp = customer.IP ? customer.IP.trim() : null;
      const syncdbPort = customer.PORT ? parseInt(customer.PORT.trim()) : null;

      if (!syncdbIp) {
        const errMsg = "IP is null for a customer entry";
        addLog("ERROR", errMsg, { customer });
        errors.push(errMsg);
        continue;
      }
      if (!syncdbPort) {
        const errMsg = `Port is null or invalid for IP: ${syncdbIp}`;
        addLog("ERROR", errMsg);
        errors.push(errMsg);
        continue;
      }

      try {
        await mssql.close();
        const syncdbConfig = {
          user: process.env.DB_USER,
          password: process.env.DB_PASSWORD,
          server: syncdbIp,
          database: process.env.DB_DATABASE2,
          options: {
          encrypt: false,
          trustServerCertificate: true,
          },
          port: syncdbPort,
        };

        addLog("INFO", "Connecting to sync DB", {
          server: syncdbConfig.server,
          port: syncdbConfig.port,
        });
        await mssql.connect(syncdbConfig);
        addLog("SUCCESS", "Connected to sync DB", {
          server: syncdbConfig.server,
          port: syncdbConfig.port,
        });

        const users = await userDetails();
        if (!users || users.length === 0) {
          const msg = `No users found for IP: ${syncdbIp}`;
          addLog("WARN", msg);
          errors.push(msg);
          continue;
        }

        const payments = await userPaymentDetails();
        if (payments.error) {
          addLog("ERROR", payments.error);
          errors.push(payments.error);
          continue;
        }

        const agent = new https.Agent({ family: 4 });

        for (const user of users) {
          const {
            SalesTaxRate,
            OAUTH_TOKEN_URL,
            API_ENDPOINT,
            ...filteredUser
          } = user;

          const userResult = {
            AppCode: filteredUser.AppCode,
            PropertyCode: filteredUser.PropertyCode,
            ClientID: filteredUser.ClientID,
            ClientSecret: filteredUser.ClientSecret,
            POSInterfaceCode: filteredUser.POSInterfaceCode,
            BatchCode: filteredUser.BatchCode,
            PosSales: [],
          };

          for (const payment of payments) {
            const { IDX, UPLOAD, Insert_Time, ...filteredPayment } = payment;

            const formattedDate = new Date(payment.ReceiptDate)
              .toLocaleDateString("en-GB")
              .replace(/\//g, "/");

            const formattedTime = new Date(payment.ReceiptTime).toLocaleTimeString(
              "en-GB",
              { hour12: false }
            );

            const newPaymentDetails = {
              PropertyCode: filteredUser.PropertyCode,
              POSInterfaceCode: filteredUser.POSInterfaceCode,
              ...filteredPayment,
              ReceiptDate: formattedDate,
              ReceiptTime: formattedTime,
            };

            const items = await userItemsDetails(
              payment.ReceiptDate,
              payment.ReceiptNo
            );
            if (items.error) {
              addLog("ERROR", items.error, {
                ReceiptDate: payment.ReceiptDate,
                ReceiptNo: payment.ReceiptNo,
              });
              errors.push(items.error);
            }

            const paymentWithItems = {
              ...newPaymentDetails,
              Items: items.error ? [] : items,
            };

            userResult.PosSales.push(trimObjectStrings(paymentWithItems));
          }

          const token = await getAccessToken(user);
          if (!token) {
          const errorMsg = `Skipping API call for user ${user.AppCode} due to token error.`;
            addLog("ERROR", errorMsg);
            errors.push(errorMsg);
            continue;
          }

          const requestBody = JSON.stringify(
            trimObjectStrings(userResult),
            null,
            2
          );

          try {
            addLog("INFO", "Calling external API", {
              AppCode: user.AppCode,
              endpoint: user.API_ENDPOINT,
            });

            const response = await axios.post(user.API_ENDPOINT, requestBody, {
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
                Accept: "application/json",
              },
              httpsAgent: agent,
              transformRequest: [(data) => data],
              timeout: 10000,
            });

            addLog("SUCCESS", "API Call Successful", {
              AppCode: user.AppCode,
            });
            apiResponses.push(response.data);
          } catch (error) {
            const errorMessage = `API Call Failed for user ${user.AppCode}: ${
              error.response?.data || error.message
            }`;
            addLog("ERROR", errorMessage);
            errors.push(errorMessage);
            apiResponses.push({ error: errorMessage });
          }
        }

        await mssql.close();
        addLog("INFO", "Closed connection for this customer", {
          server: syncdbIp,
        });
      } catch (err) {
        const errMsg = `Database Connection Error for IP ${syncdbIp}: ${err.message}`;
        addLog("ERROR", errMsg);
        errors.push(errMsg);
      }
    }

    addLog("INFO", "syncDB finished");
    return { responses: apiResponses, errors, logs };
  } catch (error) {
    addLog("ERROR", "Unexpected error in syncDB", { error: error.message });
    return { responses: [], errors: [error.message], logs };
  }
}

exports.syncDatabases = async (req, res) => {
  
    try {
      const responses = await syncDB();

      if (responses.responses[0]?.returnStatus === "Success") {
        addLog("SUCCESS", "Database sync completed successfully");

        const updateTableResult = await updateTables();
        if (updateTableResult.status === "SUCCESS") {
          addLog("SUCCESS", "Tables updated successfully", updateTableResult);
        } else {
          addLog("ERROR", "Error updating tables", updateTableResult);
        }
      } else {
        addLog("WARN", "Database sync had some issues", {
          responses: responses.responses,
          errors: responses.errors,
        });
      }
    } catch (error) {
      addLog("ERROR", "Database sync failed", { error: error.message });
    }

      const lastSuccessErrorLog = [...logs]
  .reverse()
  .find((log) => log.status === "ERROR" || log.status === "SUCCESS");

let status = "";
let message = "";
let context = {};

if (lastSuccessErrorLog) {
  status = lastSuccessErrorLog.status;
  message = lastSuccessErrorLog.message;
  context = lastSuccessErrorLog.context;
} 
else {
  const lastWarnInfoLog = [...logs]
  .reverse()
  .find((log) => log.status === "WARN" || log.status === "INFO");
  
  status = lastWarnInfoLog ? lastWarnInfoLog.status : "INFO";
  message = lastWarnInfoLog ? lastWarnInfoLog.message : "No logs found";
  context = lastWarnInfoLog ? lastWarnInfoLog.context : {};
}
      const insertRequest = new mssql.Request();
      insertRequest.input("status", mssql.NChar(10), status);
      insertRequest.input("message", mssql.NChar(500), message);
      insertRequest.input("context", mssql.NChar(1000),  JSON.stringify(context));

      await insertRequest.query(`
        INSERT INTO tb_SYNC_LOG 
        (STATUS, MESSAGE, CONTEXT)
        VALUES (@status , @message, @context)
      `);

      await mssql.close();
};