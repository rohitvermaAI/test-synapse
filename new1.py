import pyodbc
from fastapi import FastAPI, HTTPException, Query

app = FastAPI()

# Synapse connection details
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:jindalpoc.sql.azuresynapse.net,1433;"
    "Database=SQL_db;"
    "Uid=sqladminuser;"
    "Pwd=Lockdown@2030;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

@app.get("/query")
async def run_query(sql: str = Query(..., description="SQL query to execute")):
    try:
        # Use context manager for connection and cursor
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()  # Fetch rows before closing cursor

                # Format the results
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in rows]
                return {"data": results}

    except pyodbc.Error as e:
        # Catch and log specific ODBC errors
        return {"error": str(e)}
    except Exception as e:
        # Catch any other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
