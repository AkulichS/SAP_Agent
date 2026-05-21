import json
from mcp.server.fastmcp import FastMCP


mcp = FastMCP("TOOL_READ_JOB_SPOOL")

@mcp.tool()
def read_job_spool(sap_conn, job_result):
    
    data = json.loads(job_result['EV_RESULT_JSON'])

    return sap_conn.call('ZFI_AI_PERIOD_CLOSE_EXEC',
        IV_ACTION_TYPE = 'TOOL_READ_JOB_SPOOL',
        IV_OBJECT_NAME = '',
        IV_PARAMS_JSON = json.dumps(
            {"JOBNAME": data["jobname"], "JOBCOUNT": data["jobcount"]}        
        )
    )