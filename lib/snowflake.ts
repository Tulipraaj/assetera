import snowflake from 'snowflake-sdk';

if (!process.env.SNOWFLAKE_ACCOUNT) throw new Error('SNOWFLAKE_ACCOUNT is required');
if (!process.env.SNOWFLAKE_USER) throw new Error('SNOWFLAKE_USER is required');
if (!process.env.SNOWFLAKE_PASSWORD) throw new Error('SNOWFLAKE_PASSWORD is required');
if (!process.env.SNOWFLAKE_WAREHOUSE) throw new Error('SNOWFLAKE_WAREHOUSE is required');
if (!process.env.SNOWFLAKE_DATABASE) throw new Error('SNOWFLAKE_DATABASE is required');
if (!process.env.SNOWFLAKE_SCHEMA) throw new Error('SNOWFLAKE_SCHEMA is required');

export const executeQuery = async (query: string, binds: any[] = []) => {
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT!,
    username: process.env.SNOWFLAKE_USER!,
    password: process.env.SNOWFLAKE_PASSWORD!,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
    database: process.env.SNOWFLAKE_DATABASE!,
    schema: process.env.SNOWFLAKE_SCHEMA!,
  });

  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('Snowflake connection error:', err);
        reject(err);
        return;
      }

      conn.execute({
        sqlText: query,
        binds: binds,
        complete: function(err, stmt, rows) {
          // Always destroy the connection when the query is complete
          conn.destroy((destroyErr) => {
            if (destroyErr) {
              console.error('Error destroying connection:', destroyErr);
            }
            
            if (err) {
              console.error('Snowflake query error:', err);
              reject(err);
            } else {
              resolve(rows || []);
            }
          });
        },
      });
    });
  });
};
