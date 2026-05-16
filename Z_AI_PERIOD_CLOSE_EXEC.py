*&============================================================*
*& Function Module : Z_AI_PERIOD_CLOSE_EXEC
*& Function Group  : ZPAI_PERIOD  (create via SE80)
*& Description     : Universal RFC executor for AI Period Closing Agent
*& Author          : Generated for AI Agent PoC
*& Version         : 1.0
*&============================================================*
*& Supported IV_ACTION_TYPE values:
*&   FM           - Dynamic Function Module / BAPI call
*&   SUBMIT       - Background program submission (async or sync)
*&   BDC          - Batch Data Communication (CALL TRANSACTION)
*&   STATUS_CHECK - Direct table read via RFC_READ_TABLE
*&   JOB_STATUS   - Poll background job status
*&============================================================*
*&
*& ---- SETUP INSTRUCTIONS ----
*& 1. Create Function Group ZPAI_PERIOD (SE80)
*& 2. In TOP include (LZPAI_PERIODTOP), add TYPE definitions from
*&    "=== TOP INCLUDE ===" section below
*& 3. Create Function Module Z_AI_PERIOD_CLOSE_EXEC with the
*&    interface defined in "=== FUNCTION MODULE INTERFACE ==="
*& 4. Paste the FUNCTION body and all FORMs into the generated include
*& 5. Activate everything
*&
*& ---- REQUIRED AUTHORIZATIONS ----
*&   S_RFC        : Execute RFC (this FM)
*&   S_BTCH_ADM   : Background job management (for SUBMIT handler)
*&   S_TABU_DIS   : Display table access (for STATUS_CHECK)
*&   + Transaction-specific auth objects per step
*&============================================================*


*&============================================================*
*& === TOP INCLUDE (LZPAI_PERIODTOP) ===
*&============================================================*

TYPES:
  "--- Simple key-value pair (used by FM handler) ---
  BEGIN OF ty_zai_kv,
    name  TYPE string,
    value TYPE string,
  END OF ty_zai_kv,
  ty_zai_kv_tab TYPE STANDARD TABLE OF ty_zai_kv WITH DEFAULT KEY,

  "--- Selection parameter for SUBMIT handler ---
  "    Mirrors RSPARAMS but with string LOW/HIGH for JSON flexibility
  BEGIN OF ty_zai_selparam,
    selname TYPE char8,    "SELECT-OPTION or PARAMETER name in program
    kind    TYPE char1,    "P=Parameter, S=Select-option
    sign    TYPE char1,    "I=Include, E=Exclude
    option  TYPE char2,    "EQ, BT, CP, GE, LE, NE, ...
    low     TYPE string,   "Low value
    high    TYPE string,   "High value (only for kind=S, option=BT)
  END OF ty_zai_selparam,
  ty_zai_selparam_tab TYPE STANDARD TABLE OF ty_zai_selparam WITH DEFAULT KEY,

  "--- BDC field within a screen ---
  BEGIN OF ty_zai_bdc_field,
    fnam TYPE fnam_4,      "Field name on screen
    fval TYPE string,      "Value to set
  END OF ty_zai_bdc_field,
  ty_zai_bdc_field_tab TYPE STANDARD TABLE OF ty_zai_bdc_field WITH DEFAULT KEY,

  "--- BDC screen definition ---
  BEGIN OF ty_zai_bdc_screen,
    screen TYPE string,    "Format: 'PROGRAM_NAME DYNPRO' e.g. 'SAPMF01A 0100'
    fields TYPE ty_zai_bdc_field_tab,
  END OF ty_zai_bdc_screen,
  ty_zai_bdc_screen_tab TYPE STANDARD TABLE OF ty_zai_bdc_screen WITH DEFAULT KEY,

  "--- Job status query params ---
  BEGIN OF ty_zai_job_params,
    jobname  TYPE btcjob,
    jobcount TYPE btcjobcnt,
  END OF ty_zai_job_params,

  "--- Status check query params ---
  BEGIN OF ty_zai_check_params,
    where    TYPE string,  "WHERE clause e.g. "BUKRS EQ '1000' AND GJAHR EQ '2024'"
    fields   TYPE string,  "Comma-separated field list e.g. "KOKRS,GJAHR,LFMON"
    max_rows TYPE i,       "Row limit (default 100)
  END OF ty_zai_check_params,

  "--- FM/BAPI call params ---
  BEGIN OF ty_zai_fm_params,
    commit_work TYPE abap_bool,  "X = call BAPI_TRANSACTION_COMMIT after FM
  END OF ty_zai_fm_params.


*&============================================================*
*& === FUNCTION MODULE INTERFACE ===
*&============================================================*

FUNCTION z_ai_period_close_exec.
*"--------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IV_ACTION_TYPE) TYPE  STRING
*"       " Allowed: FM | BAPI | SUBMIT | BDC | STATUS_CHECK | JOB_STATUS
*"     VALUE(IV_OBJECT_NAME) TYPE  STRING
*"       " FM name | Program name | Tcode | Table name
*"     VALUE(IV_PARAMS_JSON) TYPE  STRING
*"       " Input parameters as JSON string (format per action type below)
*"       "
*"       " FM/BAPI:       {"PARAM1":"val1","PARAM2":"val2","__commit":"X"}
*"       " SUBMIT:        [{"selname":"KOKRS","kind":"P","low":"1000"},...]
*"       " BDC:           [{"screen":"PROG DYNR","fields":[{"fnam":"F","fval":"V"}]}]
*"       " STATUS_CHECK:  {"where":"BUKRS EQ '1000'","fields":"F1,F2","max_rows":50}
*"       " JOB_STATUS:    {"jobname":"MYJOB","jobcount":"12345678"}
*"       "
*"     VALUE(IV_ASYNC) TYPE  ABAP_BOOL DEFAULT ABAP_FALSE
*"       " X = Submit SUBMIT-type steps as background job (returns EV_JOB_ID)
*"     VALUE(IV_TEST_RUN) TYPE  ABAP_BOOL DEFAULT ABAP_FALSE
*"       " X = Test/simulation mode — no data changes committed
*"  EXPORTING
*"     VALUE(EV_STATUS) TYPE  STRING
*"       " S=Success  E=Error  W=Warning  A=Async(job submitted/running)
*"     VALUE(EV_RESULT_JSON) TYPE  STRING
*"       " Execution result as JSON — parsed by LangGraph node for validation
*"     VALUE(EV_JOB_ID) TYPE  STRING
*"       " Job identifier (async): format "JOBNAME|JOBCOUNT"
*"       " Use with JOB_STATUS action to poll completion
*"     VALUE(EV_MESSAGE) TYPE  STRING
*"       " Single human-readable summary (for LLM error analysis)
*"  TABLES
*"     ET_MESSAGES STRUCTURE  BAPIRET2
*"       " All messages from execution — key input for LLM validation node
*"--------------------------------------------------------------------

  DATA: lv_action TYPE string,
        lv_object TYPE string.

  CLEAR: ev_status, ev_result_json, ev_job_id, ev_message.
  REFRESH et_messages.

  lv_action = to_upper( iv_action_type ).
  lv_object = to_upper( iv_object_name ).

  "--- Route to the correct handler ---
  CASE lv_action.

    WHEN 'FM' OR 'BAPI'.
      PERFORM zai_execute_fm
        USING    lv_object
                 iv_params_json
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 ev_message
                 et_messages.

    WHEN 'SUBMIT'.
      PERFORM zai_execute_submit
        USING    lv_object
                 iv_params_json
                 iv_async
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 ev_job_id
                 ev_message
                 et_messages.

    WHEN 'BDC'.
      PERFORM zai_execute_bdc
        USING    lv_object
                 iv_params_json
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 ev_message
                 et_messages.

    WHEN 'STATUS_CHECK'.
      PERFORM zai_execute_status_check
        USING    lv_object
                 iv_params_json
        CHANGING ev_status
                 ev_result_json
                 ev_message
                 et_messages.

    WHEN 'JOB_STATUS'.
      PERFORM zai_execute_job_status
        USING    iv_params_json
        CHANGING ev_status
                 ev_result_json
                 ev_message
                 et_messages.

    WHEN OTHERS.
      ev_status  = 'E'.
      ev_message = 'Unknown IV_ACTION_TYPE: ' && iv_action_type.
      PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.

  ENDCASE.

ENDFUNCTION.


*&============================================================*
*& HANDLER 1: FM / BAPI
*&============================================================*
*
*  Calls any RFC-enabled function module dynamically.
*  Uses FUNCTION_IMPORT_INTERFACE for RTTI and
*  CALL FUNCTION ... PARAMETER-LIST for dynamic invocation.
*
*  JSON format for IV_PARAMS_JSON:
*    {
*      "COMPANYCODE": "1000",
*      "FISCALYEAR":  "2024",
*      "PERIOD":      "12",
*      "__commit":    "X"     <- optional: triggers BAPI_TRANSACTION_COMMIT
*    }
*
*  Example call (Python / LangGraph side):
*    rfc.call('Z_AI_PERIOD_CLOSE_EXEC',
*      IV_ACTION_TYPE='FM',
*      IV_OBJECT_NAME='BAPI_ACC_DOCUMENT_POST',
*      IV_PARAMS_JSON='{"COMPANYCODE":"1000","FISCALYEAR":"2024"}')
*&============================================================*
FORM zai_execute_fm
  USING    iv_funcname    TYPE string
           iv_params_json TYPE string
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE string
           ev_result_json TYPE string
           ev_message     TYPE string
           et_messages    TYPE bapiret2_t.

  "--- RTTI metadata tables from FUNCTION_IMPORT_INTERFACE ---
  DATA: lt_exc_list   TYPE STANDARD TABLE OF rsexc WITH DEFAULT KEY,
        lt_imp_params TYPE STANDARD TABLE OF rspar WITH DEFAULT KEY,  "FM Importing
        lt_exp_params TYPE STANDARD TABLE OF rspar WITH DEFAULT KEY,  "FM Exporting
        lt_tab_params TYPE STANDARD TABLE OF rstbl WITH DEFAULT KEY,  "FM Tables
        lt_chg_params TYPE STANDARD TABLE OF rspar WITH DEFAULT KEY.  "FM Changing

  "--- Dynamic call infrastructure ---
  DATA: lt_fparams   TYPE abap_func_parmbind_tab,
        lt_fexcep    TYPE abap_func_excpbind_tab,
        ls_fparam    TYPE abap_func_parmbind,
        ls_fexcep    TYPE abap_func_excpbind,
        lr_data      TYPE REF TO data.

  DATA: lv_funcname  TYPE rs38l_fnam,
        lv_commit    TYPE abap_bool.

  "--- Input key-value map from JSON ---
  DATA: lt_kv        TYPE ty_zai_kv_tab,
        ls_kv        TYPE ty_zai_kv.

  "--- Result key-value map ---
  DATA: lt_result    TYPE ty_zai_kv_tab.

  FIELD-SYMBOLS: <fs_val>  TYPE any,
                 <fs_ret>  TYPE bapiret2,
                 <fs_rett> TYPE ANY TABLE.

  "--- Step 1: Check for special meta-params in JSON ---
  PERFORM zai_parse_flat_json USING iv_params_json CHANGING lt_kv.

  READ TABLE lt_kv INTO ls_kv WITH KEY name = '__commit'.
  IF sy-subrc = 0.
    lv_commit = abap_true.
    DELETE lt_kv WHERE name = '__commit'.
  ENDIF.

  "--- Step 2: Get FM interface via RTTI ---
  lv_funcname = iv_funcname.

  CALL FUNCTION 'FUNCTION_IMPORT_INTERFACE'
    EXPORTING
      funcname           = lv_funcname
    TABLES
      exception_list     = lt_exc_list
      export_parameter   = lt_exp_params
      import_parameter   = lt_imp_params
      table_parameter    = lt_tab_params
      changing_parameter = lt_chg_params
    EXCEPTIONS
      error_message      = 1
      function_not_exist = 2
      invalid_name       = 3
      OTHERS             = 4.

  IF sy-subrc <> 0.
    ev_status  = 'E'.
    ev_message = 'FM not found or not RFC-enabled: ' && iv_funcname.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Step 3: Build PARAMETER-LIST for FM Importing (caller's Exporting) ---
  LOOP AT lt_imp_params INTO DATA(ls_imp).
    READ TABLE lt_kv INTO ls_kv WITH KEY name = ls_imp-parameter.
    IF sy-subrc <> 0.
      CONTINUE.  "Parameter not in config — use FM default
    ENDIF.

    TRY.
        CREATE DATA lr_data TYPE (ls_imp-typ).
      CATCH cx_root.
        "Unknown type — fall back to string
        CREATE DATA lr_data TYPE string.
    ENDTRY.

    ASSIGN lr_data->* TO <fs_val>.
    <fs_val> = ls_kv-value.  "Implicit string-to-type conversion

    ls_fparam-name  = ls_imp-parameter.
    ls_fparam-kind  = abap_func_exporting.  "Importing in FM = Exporting in caller
    ls_fparam-value = lr_data.
    APPEND ls_fparam TO lt_fparams.
  ENDLOOP.

  "--- Step 4: Prepare PARAMETER-LIST for FM Exporting (to capture output) ---
  LOOP AT lt_exp_params INTO DATA(ls_exp).
    TRY.
        CREATE DATA lr_data TYPE (ls_exp-typ).
      CATCH cx_root.
        CREATE DATA lr_data TYPE string.
    ENDTRY.

    ls_fparam-name  = ls_exp-parameter.
    ls_fparam-kind  = abap_func_importing.  "Exporting in FM = Importing in caller
    ls_fparam-value = lr_data.
    APPEND ls_fparam TO lt_fparams.
  ENDLOOP.

  "--- Step 5: Prepare TABLE params (including RETURN for BAPIs) ---
  LOOP AT lt_tab_params INTO DATA(ls_tab).
    IF ls_tab-dbstruct = 'BAPIRET2' OR ls_tab-parameter = 'RETURN'.
      "Reserve a BAPIRET2 table to capture messages
      DATA: lt_return TYPE bapiret2_t.
      GET REFERENCE OF lt_return INTO lr_data.
    ELSE.
      CREATE DATA lr_data TYPE STANDARD TABLE OF (ls_tab-dbstruct).
    ENDIF.

    ls_fparam-name  = ls_tab-parameter.
    ls_fparam-kind  = abap_func_tables.
    ls_fparam-value = lr_data.
    APPEND ls_fparam TO lt_fparams.
  ENDLOOP.

  "--- Step 6: Set OTHERS exception ---
  ls_fexcep-name  = 'OTHERS'.
  ls_fexcep-value = 99.
  APPEND ls_fexcep TO lt_fexcep.

  "--- Step 7: Dynamic FM execution ---
  CALL FUNCTION lv_funcname
    PARAMETER-LIST  lt_fparams
    EXCEPTION-TABLE lt_fexcep.

  IF sy-subrc = 99.
    ev_status  = 'E'.
    ev_message = iv_funcname && ' raised unhandled exception'.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Step 8: Harvest RETURN table messages (BAPI pattern) ---
  LOOP AT lt_return ASSIGNING <fs_ret>.
    APPEND <fs_ret> TO et_messages.
  ENDLOOP.

  "--- Step 9: Collect scalar Exporting params for result JSON ---
  LOOP AT lt_fparams INTO ls_fparam WHERE kind = abap_func_importing.
    ASSIGN ls_fparam-value->* TO <fs_val>.
    ls_kv-name  = ls_fparam-name.
    ls_kv-value = |{ <fs_val> }|.
    APPEND ls_kv TO lt_result.
  ENDLOOP.

  "--- Step 10: Test run — no commit ---
  IF iv_test_run = abap_true.
    lv_commit = abap_false.
  ENDIF.

  "--- Step 11: Optional BAPI commit ---
  IF lv_commit = abap_true.
    DATA: ls_ret TYPE bapiret2.
    CALL FUNCTION 'BAPI_TRANSACTION_COMMIT'
      EXPORTING  wait   = 'X'
      IMPORTING  return = ls_ret.
    IF ls_ret-type CA 'EAX'.
      APPEND ls_ret TO et_messages.
    ENDIF.
  ENDIF.

  "--- Step 12: Determine overall status ---
  READ TABLE et_messages TRANSPORTING NO FIELDS
    WITH KEY type = 'E'.
  IF sy-subrc = 0.
    ev_status  = 'E'.
    ev_message = iv_funcname && ' completed with errors'.
  ELSE.
    READ TABLE et_messages TRANSPORTING NO FIELDS
      WITH KEY type = 'W'.
    IF sy-subrc = 0.
      ev_status  = 'W'.
      ev_message = iv_funcname && ' completed with warnings'.
    ELSE.
      ev_status  = 'S'.
      ev_message = iv_funcname && ' completed successfully'.
    ENDIF.
  ENDIF.

  PERFORM zai_build_json USING lt_result CHANGING ev_result_json.

ENDFORM.


*&============================================================*
*& HANDLER 2: SUBMIT — Background Program Submission
*&============================================================*
*
*  Submits any ABAP report/program.
*  In async mode (IV_ASYNC=X) schedules a background job
*  and returns EV_JOB_ID = "JOBNAME|JOBCOUNT" for polling.
*  In sync mode executes inline (for short reports only).
*
*  JSON format for IV_PARAMS_JSON (array of selection params):
*    [
*      {"selname":"KOKRS",    "kind":"P", "low":"1000"},
*      {"selname":"ABPER",    "kind":"P", "low":"12"},
*      {"selname":"ABPIS",    "kind":"P", "low":"12"},
*      {"selname":"ABGJAHR",  "kind":"P", "low":"2024"},
*      {"selname":"ZCYKL",    "kind":"S", "sign":"I","option":"EQ","low":"CYCLE01"}
*    ]
*
*  SAP programs for common period-closing transactions:
*    KO8G  (CO Assessment)          -> RKABL000
*    KSW5  (Indirect Activity)      -> RKAIB000
*    KSC5  (Cost Center Settlement) -> RKASL000
*    KSII  (Act. Price Calculation) -> RKSST000
*    CO88  (Order Settlement)       -> RAABST02
*    AFAB  (Depreciation Run)       -> RABUCH00
*    F.16  (Carry Forward)          -> RFBILA00  (check per system)
*&============================================================*
FORM zai_execute_submit
  USING    iv_progname    TYPE string
           iv_params_json TYPE string
           iv_async       TYPE abap_bool
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE string
           ev_result_json TYPE string
           ev_job_id      TYPE string
           ev_message     TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: lt_selopts  TYPE ty_zai_selparam_tab,
        ls_selopt   TYPE ty_zai_selparam.
  DATA: lt_rsparams TYPE rsparams_tt,
        ls_rsparam  TYPE rsparams.
  DATA: lv_progname TYPE sy-repid,
        lv_jobname  TYPE btcjob,
        lv_jobcount TYPE btcjobcnt.

  "--- Parse JSON array of selection parameters ---
  /ui2/cl_json=>deserialize(
    EXPORTING
      json             = iv_params_json
      pretty_name      = /ui2/cl_json=>pretty_mode-camel_case
    CHANGING
      data             = lt_selopts ).

  "--- Build standard RSPARAMS table ---
  LOOP AT lt_selopts INTO ls_selopt.
    CLEAR ls_rsparam.
    ls_rsparam-selname = ls_selopt-selname.
    ls_rsparam-kind    = COND #(
      WHEN ls_selopt-kind   IS INITIAL THEN 'P'
      ELSE ls_selopt-kind ).
    ls_rsparam-sign    = COND #(
      WHEN ls_selopt-sign   IS INITIAL THEN 'I'
      ELSE ls_selopt-sign ).
    ls_rsparam-option  = COND #(
      WHEN ls_selopt-option IS INITIAL THEN 'EQ'
      ELSE ls_selopt-option ).
    ls_rsparam-low     = ls_selopt-low.
    ls_rsparam-high    = ls_selopt-high.
    APPEND ls_rsparam TO lt_rsparams.
  ENDLOOP.

  "--- Inject TESTLAUF / SIMULATION flag if test run ---
  IF iv_test_run = abap_true.
    READ TABLE lt_rsparams TRANSPORTING NO FIELDS
      WITH KEY selname = 'TESTLAUF'.
    IF sy-subrc <> 0.
      CLEAR ls_rsparam.
      ls_rsparam-selname = 'TESTLAUF'.
      ls_rsparam-kind    = 'P'.
      ls_rsparam-sign    = 'I'.
      ls_rsparam-option  = 'EQ'.
      ls_rsparam-low     = 'X'.
      APPEND ls_rsparam TO lt_rsparams.
    ENDIF.
  ENDIF.

  lv_progname = iv_progname.

  "============================================================
  "--- SYNCHRONOUS execution (short reports only) ---
  "============================================================
  IF iv_async = abap_false.
    SUBMIT (lv_progname)
      WITH SELECTION-TABLE lt_rsparams
      AND RETURN.

    IF sy-subrc <> 0.
      ev_status  = 'E'.
      ev_message = 'SUBMIT failed for program: ' && iv_progname.
      PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
      RETURN.
    ENDIF.

    ev_status  = 'S'.
    ev_message = 'Program ' && iv_progname && ' executed synchronously'.
    ev_result_json = '{"status":"completed"' &&
                     ',"mode":"sync"' &&
                     ',"program":"' && iv_progname && '"}'.
    PERFORM zai_add_msg USING 'S' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "============================================================
  "--- ASYNCHRONOUS: background job submission ---
  "============================================================

  "--- Generate unique job name (max 32 chars) ---
  DATA: lv_ts TYPE string.
  lv_ts = sy-uzeit.
  CONCATENATE 'ZAI_PC_' iv_progname(8) '_' lv_ts(6)
    INTO lv_jobname.

  "--- Open job slot ---
  CALL FUNCTION 'JOB_OPEN'
    EXPORTING
      jobname          = lv_jobname
    IMPORTING
      jobcount         = lv_jobcount
    EXCEPTIONS
      cant_create_job  = 1
      invalid_job_data = 2
      jobname_missing  = 3
      OTHERS           = 4.

  IF sy-subrc <> 0.
    ev_status  = 'E'.
    ev_message = 'Cannot create background job — check S_BTCH_ADM auth'.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Submit report step to the open job ---
  SUBMIT (lv_progname)
    WITH SELECTION-TABLE lt_rsparams
    VIA JOB     lv_jobname
    NUMBER      lv_jobcount
    AND RETURN.

  IF sy-subrc <> 0.
    ev_status  = 'E'.
    ev_message = 'Cannot attach program ' && iv_progname && ' to job'.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Release job for immediate execution ---
  CALL FUNCTION 'JOB_CLOSE'
    EXPORTING
      jobcount             = lv_jobcount
      jobname              = lv_jobname
      strtimmed            = 'X'       "Start immediately
    EXCEPTIONS
      cant_start_immediate = 1
      invalid_startdate    = 2
      jobname_missing      = 3
      job_close_failed     = 4
      job_nosteps          = 5
      job_notex            = 6
      lock_failed          = 7
      OTHERS               = 8.

  IF sy-subrc <> 0.
    ev_status  = 'E'.
    ev_message = 'Cannot release job ' && lv_jobname.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Return job ID for polling by JOB_STATUS handler ---
  CONCATENATE lv_jobname '|' lv_jobcount
    INTO ev_job_id.

  ev_status  = 'A'.  "Async — job submitted
  ev_message = 'Job submitted: ' && lv_jobname && ' / ' && lv_jobcount.

  ev_result_json = '{"status":"submitted"' &&
                   ',"mode":"async"' &&
                   ',"program":"' && iv_progname && '"' &&
                   ',"jobname":"' && lv_jobname && '"' &&
                   ',"jobcount":"' && lv_jobcount && '"}'.

  PERFORM zai_add_msg USING 'S' ev_message CHANGING et_messages.

ENDFORM.


*&============================================================*
*& HANDLER 3: BDC — Batch Data Communication
*&============================================================*
*
*  Use as LAST RESORT for transactions without FM/BAPI.
*  Fragile: screen layouts change with Support Packages.
*  Where possible, replace with SUBMIT or FM approach.
*
*  JSON format for IV_PARAMS_JSON:
*  [
*    {
*      "screen": "SAPMF01A 0100",
*      "fields": [
*        {"fnam": "RF01A-POPER",  "fval": "12"},
*        {"fnam": "RF01A-GJAHR",  "fval": "2024"},
*        {"fnam": "BDC_OKCODE",   "fval": "/00"}
*      ]
*    },
*    {
*      "screen": "SAPMF01A 0100",
*      "fields": [
*        {"fnam": "BDC_OKCODE",   "fval": "=SAVE"}
*      ]
*    }
*  ]
*
*  Example transactions typically requiring BDC:
*    OB52  - Open/close FI posting periods
*    MMPI  - Initialize MM period (old approach)
*    OKP1  - CO period lock
*&============================================================*
FORM zai_execute_bdc
  USING    iv_tcode       TYPE string
           iv_params_json TYPE string
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE string
           ev_result_json TYPE string
           ev_message     TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: lt_screens TYPE ty_zai_bdc_screen_tab,
        ls_screen  TYPE ty_zai_bdc_screen.
  DATA: lt_bdcdata TYPE TABLE OF bdcdata,
        ls_bdcdata TYPE bdcdata.
  DATA: lt_messtab TYPE TABLE OF bdcmsgcoll,
        ls_messtab TYPE bdcmsgcoll.
  DATA: lv_tcode   TYPE tcode,
        lv_mode    TYPE c LENGTH 1,
        lv_prog    TYPE sy-repid,
        lv_dynr    TYPE sy-dynnr.
  DATA: ls_bapiret TYPE bapiret2.

  "--- Parse JSON screen/field definition ---
  /ui2/cl_json=>deserialize(
    EXPORTING json = iv_params_json
    CHANGING  data = lt_screens ).

  IF lt_screens IS INITIAL.
    ev_status  = 'E'.
    ev_message = 'BDC screen data is empty — check IV_PARAMS_JSON'.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Build BDCDATA table ---
  LOOP AT lt_screens INTO ls_screen.
    CLEAR ls_bdcdata.

    "Set screen header (DYNBEGIN)
    SPLIT ls_screen-screen AT ' '
      INTO lv_prog lv_dynr.
    ls_bdcdata-program  = lv_prog.
    ls_bdcdata-dynpro   = lv_dynr.
    ls_bdcdata-dynbegin = 'X'.
    APPEND ls_bdcdata TO lt_bdcdata.

    "Set field values for this screen
    LOOP AT ls_screen-fields INTO DATA(ls_field).
      CLEAR ls_bdcdata.
      ls_bdcdata-fnam = ls_field-fnam.
      ls_bdcdata-fval = ls_field-fval.
      APPEND ls_bdcdata TO lt_bdcdata.
    ENDLOOP.
  ENDLOOP.

  "--- Set display mode ---
  "  A = show all screens  (for debugging)
  "  E = show error screens only
  "  N = no screen display (production)
  lv_mode  = COND #( WHEN iv_test_run = abap_true THEN 'E' ELSE 'N' ).
  lv_tcode = iv_tcode.

  "--- Execute transaction with BDC data ---
  CALL TRANSACTION lv_tcode
    USING    lt_bdcdata
    MODE     lv_mode
    UPDATE   'S'           "Synchronous DB update
    MESSAGES INTO lt_messtab.

  "--- Convert BDCMSGCOLL to BAPIRET2 ---
  LOOP AT lt_messtab INTO ls_messtab.
    CLEAR ls_bapiret.
    ls_bapiret-type       = ls_messtab-msgtyp.
    ls_bapiret-id         = ls_messtab-msgid.
    ls_bapiret-number     = ls_messtab-msgnr.
    ls_bapiret-message_v1 = ls_messtab-msgv1.
    ls_bapiret-message_v2 = ls_messtab-msgv2.
    ls_bapiret-message_v3 = ls_messtab-msgv3.
    ls_bapiret-message_v4 = ls_messtab-msgv4.

    MESSAGE ID     ls_messtab-msgid
            TYPE   ls_messtab-msgtyp
            NUMBER ls_messtab-msgnr
            INTO   ls_bapiret-message
            WITH   ls_messtab-msgv1
                   ls_messtab-msgv2
                   ls_messtab-msgv3
                   ls_messtab-msgv4.

    APPEND ls_bapiret TO et_messages.
  ENDLOOP.

  "--- Determine overall status ---
  READ TABLE et_messages TRANSPORTING NO FIELDS WITH KEY type = 'E'.
  IF sy-subrc = 0.
    ev_status  = 'E'.
    ev_message = 'BDC ' && iv_tcode && ' completed with errors'.
  ELSE.
    ev_status  = 'S'.
    ev_message = 'BDC ' && iv_tcode && ' completed successfully'.
  ENDIF.

  ev_result_json = '{"status":"' && ev_status && '"' &&
                   ',"tcode":"' && iv_tcode && '"' &&
                   ',"message_count":"' &&
                   lines( et_messages ) && '"}'.

ENDFORM.


*&============================================================*
*& HANDLER 4: STATUS_CHECK — Direct Table Read
*&============================================================*
*
*  Reads data from any SAP transparent table using RFC_READ_TABLE.
*  Used for:
*  - Verifying period open/close state before and after steps
*  - Reading CO actual postings to validate cycle runs
*  - Checking job logs, control flags in customizing tables
*
*  JSON format for IV_PARAMS_JSON:
*    {
*      "where":    "KOKRS EQ '1000' AND GJAHR EQ '2024' AND PERAB EQ '012'",
*      "fields":   "KOKRS,GJAHR,PERAB,KSTAR,WKGBTR",
*      "max_rows": 50
*    }
*
*  Useful SAP tables for period-closing status checks:
*    MARV    - MM current posting period (fields: LFMON, LFGJA)
*    T001B   - FI permitted posting periods
*    COSP    - CO primary cost: plan/actual totals
*    COSS    - CO secondary cost totals
*    FAGLFLEXT - New GL totals
*    TKA01   - Controlling area settings
*    COKP    - CO period lock status
*&============================================================*
FORM zai_execute_status_check
  USING    iv_table       TYPE string
           iv_params_json TYPE string
  CHANGING ev_status      TYPE string
           ev_result_json TYPE string
           ev_message     TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: ls_params   TYPE ty_zai_check_params.
  DATA: lt_options  TYPE STANDARD TABLE OF rfc_db_opt,
        ls_option   TYPE rfc_db_opt.
  DATA: lt_rfc_flds TYPE STANDARD TABLE OF rfc_db_fld,
        ls_rfc_fld  TYPE rfc_db_fld.
  DATA: lt_rfc_data TYPE STANDARD TABLE OF tab512,
        ls_rfc_row  TYPE tab512.
  DATA: lt_field_names TYPE STANDARD TABLE OF string.
  DATA: lv_offset   TYPE i,
        lv_len      TYPE i,
        lv_val      TYPE string,
        lv_json     TYPE string,
        lv_rows_j   TYPE string,
        lv_sep      TYPE string,
        lv_fsep     TYPE string.

  "--- Parse query params ---
  /ui2/cl_json=>deserialize(
    EXPORTING json = iv_params_json
    CHANGING  data = ls_params ).

  IF ls_params-max_rows = 0.
    ls_params-max_rows = 100.
  ENDIF.

  "--- Build field list ---
  SPLIT ls_params-fields AT ',' INTO TABLE lt_field_names.
  LOOP AT lt_field_names INTO DATA(lv_fname).
    CONDENSE lv_fname.
    ls_rfc_fld-fieldname = lv_fname.
    APPEND ls_rfc_fld TO lt_rfc_flds.
  ENDLOOP.

  "--- Build WHERE clause ---
  IF ls_params-where IS NOT INITIAL.
    ls_option-text = ls_params-where.
    APPEND ls_option TO lt_options.
  ENDIF.

  "--- Execute RFC_READ_TABLE ---
  CALL FUNCTION 'RFC_READ_TABLE'
    EXPORTING
      query_table          = iv_table
      rowcount             = ls_params-max_rows
      no_data              = ' '
      delimiter            = '|'      "Field delimiter in output rows
    TABLES
      options              = lt_options
      fields               = lt_rfc_flds
      data                 = lt_rfc_data
    EXCEPTIONS
      table_not_available  = 1
      table_without_data   = 2
      option_not_valid     = 3
      field_not_valid      = 4
      not_authorized       = 5
      data_buffer_exceeded = 6
      OTHERS               = 7.

  CASE sy-subrc.
    WHEN 0.
      ev_status = 'S'.
    WHEN 1.
      ev_status  = 'E'.
      ev_message = 'Table not available: ' && iv_table.
      PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
      RETURN.
    WHEN 2.
      ev_status  = 'W'.
      ev_message = 'Table ' && iv_table && ' is empty for given criteria'.
      PERFORM zai_add_msg USING 'W' ev_message CHANGING et_messages.
      ev_result_json = '{"table":"' && iv_table &&
                       '","rows":[],"count":0}'.
      RETURN.
    WHEN OTHERS.
      ev_status  = 'E'.
      ev_message = 'RFC_READ_TABLE error on ' && iv_table &&
                   ', subrc=' && sy-subrc.
      PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
      RETURN.
  ENDCASE.

  "--- Build JSON array from result rows ---
  "    RFC_READ_TABLE returns pipe-delimited rows in DATA
  lv_rows_j = '['.
  lv_sep    = ''.

  LOOP AT lt_rfc_data INTO ls_rfc_row.
    DATA: lt_values TYPE STANDARD TABLE OF string,
          lv_rowjson TYPE string.

    SPLIT ls_rfc_row AT '|' INTO TABLE lt_values.

    lv_rowjson = '{'.
    lv_fsep    = ''.

    LOOP AT lt_rfc_flds INTO ls_rfc_fld.
      DATA: lv_fidx TYPE sy-tabix.
      lv_fidx = sy-tabix.
      READ TABLE lt_values INTO lv_val INDEX lv_fidx.
      IF sy-subrc = 0.
        CONDENSE lv_val.
      ELSE.
        CLEAR lv_val.
      ENDIF.

      CONCATENATE lv_rowjson lv_fsep
                  '"' ls_rfc_fld-fieldname '":'
                  '"' lv_val '"'
        INTO lv_rowjson.
      lv_fsep = ','.
    ENDLOOP.

    CONCATENATE lv_rowjson '}' INTO lv_rowjson.
    CONCATENATE lv_rows_j lv_sep lv_rowjson INTO lv_rows_j.
    lv_sep = ','.
  ENDLOOP.

  CONCATENATE lv_rows_j ']' INTO lv_rows_j.

  DATA: lv_cnt TYPE string.
  lv_cnt = lines( lt_rfc_data ).

  ev_message = 'Read ' && lv_cnt && ' row(s) from ' && iv_table.
  ev_result_json = '{"table":"' && iv_table && '"' &&
                   ',"count":' && lv_cnt &&
                   ',"rows":' && lv_rows_j && '}'.

ENDFORM.


*&============================================================*
*& HANDLER 5: JOB_STATUS — Poll Background Job
*&============================================================*
*
*  Called by LangGraph node polling loop after SUBMIT with async=True.
*  Returns EV_STATUS:
*    S = FINISHED (job done, no errors in spool)
*    E = ABORTED
*    A = RUNNING | READY | SCHEDULED (still in progress)
*
*  JSON format for IV_PARAMS_JSON:
*    {"jobname":"ZAI_PC_RKABL000_143022","jobcount":"12345678"}
*
*  Or pass EV_JOB_ID from previous SUBMIT call directly:
*    split on '|' -> jobname / jobcount
*&============================================================*
FORM zai_execute_job_status
  USING    iv_params_json TYPE string
  CHANGING ev_status      TYPE string
           ev_result_json TYPE string
           ev_message     TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: ls_job     TYPE ty_zai_job_params.
  DATA: lv_aborted TYPE c LENGTH 1,
        lv_finish  TYPE c LENGTH 1,
        lv_running TYPE c LENGTH 1,
        lv_ready   TYPE c LENGTH 1,
        lv_schedul TYPE c LENGTH 1,
        lv_state   TYPE string.

  "--- Support both JSON and "JOBNAME|JOBCOUNT" pipe format ---
  IF iv_params_json(1) = '{'.
    /ui2/cl_json=>deserialize(
      EXPORTING json = iv_params_json
      CHANGING  data = ls_job ).
  ELSE.
    "Plain pipe-separated format from EV_JOB_ID
    SPLIT iv_params_json AT '|'
      INTO ls_job-jobname ls_job-jobcount.
  ENDIF.

  "--- Query job status ---
  CALL FUNCTION 'SHOW_JOBSTATE'
    EXPORTING
      jobcount         = ls_job-jobcount
      jobname          = ls_job-jobname
    IMPORTING
      aborted          = lv_aborted
      finished         = lv_finish
      preliminary      = lv_schedul
      ready            = lv_ready
      running          = lv_running
    EXCEPTIONS
      jobcount_missing = 1
      jobname_missing  = 2
      job_notex        = 3
      OTHERS           = 4.

  IF sy-subrc <> 0.
    ev_status  = 'E'.
    ev_message = 'Job not found: ' && ls_job-jobname &&
                 ' / ' && ls_job-jobcount.
    PERFORM zai_add_msg USING 'E' ev_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Map state flags to a single status ---
  IF lv_finish = 'X'.
    lv_state   = 'FINISHED'.
    ev_status  = 'S'.
  ELSEIF lv_aborted = 'X'.
    lv_state   = 'ABORTED'.
    ev_status  = 'E'.
  ELSEIF lv_running = 'X'.
    lv_state   = 'RUNNING'.
    ev_status  = 'A'.
  ELSEIF lv_ready = 'X'.
    lv_state   = 'READY'.
    ev_status  = 'A'.
  ELSE.
    lv_state   = 'SCHEDULED'.
    ev_status  = 'A'.
  ENDIF.

  ev_message = 'Job ' && ls_job-jobname && ': ' && lv_state.
  ev_result_json = '{"jobname":"'  && ls_job-jobname  && '"' &&
                   ',"jobcount":"' && ls_job-jobcount && '"' &&
                   ',"state":"'    && lv_state         && '"' &&
                   ',"status":"'   && ev_status         && '"}'.

ENDFORM.


*&============================================================*
*& HELPERS
*&============================================================*

*&--------------------------------------------------------------------*
*& FORM zai_parse_flat_json
*&  Parses a simple flat JSON object {"K1":"V1","K2":"V2",...}
*&  into an internal name-value table.
*&  Note: Works for scalar string values only.
*&        For nested structures use /ui2/cl_json=>deserialize directly.
*&--------------------------------------------------------------------*
FORM zai_parse_flat_json
  USING    iv_json TYPE string
  CHANGING ct_kv   TYPE ty_zai_kv_tab.

  DATA: lv_json   TYPE string,
        lv_len    TYPE i,
        lv_char   TYPE c,
        lv_tok    TYPE string,
        lv_key    TYPE string,
        lv_in_str TYPE abap_bool,
        lv_is_key TYPE abap_bool,
        lv_i      TYPE i.
  DATA: ls_kv     TYPE ty_zai_kv.

  CLEAR ct_kv.
  lv_json   = iv_json.
  lv_len    = strlen( lv_json ).
  lv_is_key = abap_true.
  lv_in_str = abap_false.

  DO lv_len TIMES.
    lv_i    = sy-index - 1.
    lv_char = lv_json+lv_i(1).

    CASE lv_char.
      WHEN '"'.
        IF lv_in_str = abap_false.
          lv_in_str = abap_true.
          CLEAR lv_tok.
        ELSE.
          lv_in_str = abap_false.
          IF lv_is_key = abap_true.
            lv_key = lv_tok.
          ELSE.
            ls_kv-name  = lv_key.
            ls_kv-value = lv_tok.
            APPEND ls_kv TO ct_kv.
            CLEAR: lv_key, lv_tok.
          ENDIF.
        ENDIF.
      WHEN ':'.
        IF lv_in_str = abap_false.
          lv_is_key = abap_false.
        ELSE.
          lv_tok = lv_tok && lv_char.
        ENDIF.
      WHEN ','.
        IF lv_in_str = abap_false.
          lv_is_key = abap_true.
        ELSE.
          lv_tok = lv_tok && lv_char.
        ENDIF.
      WHEN OTHERS.
        IF lv_in_str = abap_true.
          lv_tok = lv_tok && lv_char.
        ENDIF.
    ENDCASE.
  ENDDO.

ENDFORM.


*&--------------------------------------------------------------------*
*& FORM zai_build_json
*&  Serializes a name-value table to a flat JSON object string.
*&--------------------------------------------------------------------*
FORM zai_build_json
  USING    ct_kv  TYPE ty_zai_kv_tab
  CHANGING ev_json TYPE string.

  DATA: ls_kv TYPE ty_zai_kv,
        lv_sep TYPE string.

  ev_json = '{'.
  lv_sep  = ''.

  LOOP AT ct_kv INTO ls_kv.
    CONCATENATE ev_json lv_sep
                '"' ls_kv-name '":'
                '"' ls_kv-value '"'
      INTO ev_json.
    lv_sep = ','.
  ENDLOOP.

  CONCATENATE ev_json '}' INTO ev_json.

ENDFORM.


*&--------------------------------------------------------------------*
*& FORM zai_add_msg
*&  Appends a simple message to ET_MESSAGES (BAPIRET2).
*&--------------------------------------------------------------------*
FORM zai_add_msg
  USING    iv_type    TYPE c
           iv_text    TYPE string
  CHANGING et_messages TYPE bapiret2_t.

  DATA: ls_msg TYPE bapiret2.
  ls_msg-type       = iv_type.
  ls_msg-id         = 'ZAI_PERIOD'.
  ls_msg-number     = '001'.
  ls_msg-message    = iv_text.
  ls_msg-message_v1 = iv_text.
  APPEND ls_msg TO et_messages.

ENDFORM.
