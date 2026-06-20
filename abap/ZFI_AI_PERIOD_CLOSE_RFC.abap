*&============================================================*
*& Function Module : ZFI_AI_PERIOD_CLOSE_RFC
*& Description     : Universal RFC executor for AI Period Closing Agent
*& Version         : 1.0
*&============================================================*
*& Supported IV_ACTION_TYPE values:
*&   FM           - Dynamic Function Module / BAPI call
*&   SUBMIT       - Background program submission (async or sync)
*&   BDC          - Batch Data Communication (CALL TRANSACTION)
*&   TOOLS        - Different tools like Direct table read via RFC_READ_TABLE or read spool for job
*&============================================================*

*&============================================================*
*&    Global constants (in real system: function group TOP include)
*&============================================================*
CONSTANTS:
  lc_error   TYPE c VALUE 'E',   "Error — ev_status / BAPIRET2 type
  lc_warning TYPE c VALUE 'W',   "Warning
  lc_success TYPE c VALUE 'S',   "Success
  lc_async   TYPE c VALUE 'A',   "Async — job submitted, result pending
  "--- inline-wait (sync_wait) tuning for SUBMIT with iv_async = '' ---
  lc_sync_wait_interval TYPE i VALUE 2,    "poll interval while waiting (s)
  lc_sync_wait_timeout  TYPE i VALUE 300.  "guard timeout for inline wait (s)

*&============================================================*
*&    FUNCTION MODULE INTERFACE
*&============================================================*

FUNCTION ZFI_AI_PERIOD_CLOSE_RFC.
*"----------------------------------------------------------------------
*"*"Локальный интерфейс:
*"  IMPORTING
*"     VALUE(IV_ACTION_TYPE) TYPE  STRING
*"     VALUE(IV_OBJECT_NAME) TYPE  STRING
*"     VALUE(IV_PARAMS_JSON) TYPE  STRING
*"     VALUE(IV_ASYNC) TYPE  CHAR1 OPTIONAL
*"     VALUE(IV_TEST_RUN) TYPE  CHAR1 OPTIONAL
*"  EXPORTING
*"     VALUE(EV_STATUS) TYPE  CHAR1
*"     VALUE(EV_RESULT_JSON) TYPE  STRING
*"     VALUE(EV_MESSAGE) TYPE  STRING
*"  CHANGING
*"     VALUE(ET_MESSAGES) TYPE  BAPIRET2_T
*"----------------------------------------------------------------------

  DATA: lv_action      TYPE string,
        lv_object_name TYPE sy-repid,
        lv_msg         TYPE string.

  CLEAR: ev_status, ev_result_json.
  REFRESH et_messages.

  lv_action = to_upper( iv_action_type ).
  lv_object_name = to_upper( iv_object_name ).

  "--- Route to the correct handler ---
  CASE lv_action.

    WHEN 'FM' OR 'BAPI'.
      PERFORM z_execute_fm
        USING    lv_object_name
                 iv_params_json
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 et_messages.

    WHEN 'SUBMIT'.
      PERFORM z_execute_submit
        USING    lv_object_name
                 iv_params_json
                 iv_async
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 et_messages.

    WHEN 'BDC'.
      PERFORM z_execute_bdc
        USING    lv_object_name
                 iv_params_json
                 iv_test_run
        CHANGING ev_status
                 ev_result_json
                 et_messages.

    WHEN 'TOOLS'.
      PERFORM z_execute_tool
        USING    lv_object_name
                 iv_params_json
        CHANGING ev_status
                 ev_result_json
                 et_messages.

    WHEN OTHERS.
      ev_status  = lc_error.
      lv_msg = 'Unknown IV_ACTION_TYPE: ' && iv_action_type.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.

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
*  Example call (Python / LangGraph side):
*    rfc.call('ZFI_AI_PERIOD_CLOSE_RFC',
*      IV_ACTION_TYPE='FM',
*      IV_OBJECT_NAME='BAPI_ACC_DOCUMENT_POST',
*      IV_PARAMS_JSON='{"COMPANYCODE":"1000","FISCALYEAR":"2024"}')
*&============================================================*

FORM z_execute_fm
  USING    iv_funcname    TYPE sy-repid
           iv_params_json TYPE string
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

  "--- RTTI metadata tables from FUNCTION_IMPORT_INTERFACE ---
  DATA: lt_exc_list   TYPE STANDARD TABLE OF rsexc WITH DEFAULT KEY,
        lt_imp_params TYPE STANDARD TABLE OF rsimp WITH DEFAULT KEY,  "FM Importing
        lt_exp_params TYPE STANDARD TABLE OF rsexp WITH DEFAULT KEY,  "FM Exporting
        lt_tab_params TYPE STANDARD TABLE OF rstbl WITH DEFAULT KEY,  "FM Tables
        lt_chg_params TYPE STANDARD TABLE OF rscha WITH DEFAULT KEY.  "FM Changing

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
  PERFORM z_parse_flat_json USING iv_params_json CHANGING lt_kv.

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
    ev_status  = lc_error.
    DATA(lv_msg) = 'FM not found or not RFC-enabled: ' && iv_funcname.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
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
  INSERT ls_fexcep INTO TABLE lt_fexcep.

  "--- Step 7: Dynamic FM execution ---
  CALL FUNCTION lv_funcname
    PARAMETER-TABLE  lt_fparams
    EXCEPTION-TABLE lt_fexcep.

  IF sy-subrc = 99.
    ev_status  = lc_error.
    lv_msg = iv_funcname && ' raised unhandled exception'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
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
    WITH KEY type = lc_error.
  IF sy-subrc = 0.
    ev_status  = lc_error.
    lv_msg = iv_funcname && ' completed with errors'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
  ELSE.
    READ TABLE et_messages TRANSPORTING NO FIELDS
      WITH KEY type = lc_warning.
    IF sy-subrc = 0.
      ev_status  = lc_warning.
      lv_msg = iv_funcname && ' completed with warnings'.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    ELSE.
      ev_status  = lc_success.
      lv_msg = iv_funcname && ' completed successfully'.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    ENDIF.
  ENDIF.

  PERFORM z_build_json USING lt_result CHANGING ev_result_json.

ENDFORM.


*&============================================================*
*& HANDLER 2: SUBMIT — Background Program Submission
*&============================================================*
*
*  Submits any ABAP report/program. ALWAYS runs via a background job
*  (JOB_OPEN / SUBMIT VIA JOB / JOB_CLOSE) — never an inline SUBMIT — so
*  reports that produce ALV/list output cannot raise CNTL_ERROR in the
*  GUI-less RFC context, and their output is always captured to spool.
*
*  IV_ASYNC = 'X' : return immediately (EV_STATUS = lc_async) with
*                   jobname/jobcount; the caller polls via TOOL_JOB_STATUS.
*  IV_ASYNC = ''  : wait inline until the job finishes (guarded by
*                   lc_sync_wait_timeout), then fetch the spool and return
*                   it in EV_RESULT_JSON {"...,"spool":[..lines..]}.
*
*  JSON format for IV_PARAMS_JSON (array of selection params):
*    [
*      {"selname":"KOKRS",    "kind":"P", "low":"1000"},
*      {"selname":"ABGJAHR",  "kind":"P", "low":"2024"},
*      {"selname":"ZCYKL",    "kind":"S", "sign":"I","option":"EQ","low":"CYCLE01"}
*    ]
*&============================================================*

FORM z_execute_submit
  USING    iv_progname    TYPE sy-repid
           iv_params_json TYPE string
           iv_async       TYPE abap_bool
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: lt_selopts   TYPE ty_zai_selparam_tab,
        ls_selopt    LIKE LINE OF lt_selopts,
        lt_rsparams  TYPE rsparams_tt,
        ls_rsparam   TYPE rsparams,
        lv_jobname   TYPE btcjob,
        lv_jobcount  TYPE btcjobcnt,
        lv_ts        TYPE string,
        lv_msg       TYPE string,
        "--- inline-wait (sync_wait) locals ---
        lv_state     TYPE string,
        lv_waited    TYPE i,
        lt_lines     TYPE stringtab,
        lv_subrc     TYPE i,
        lv_spoolmsg  TYPE string,
        lv_spooljson TYPE string.

  "--- Parse JSON array of selection parameters ---
  /ui2/cl_json=>deserialize( EXPORTING json = iv_params_json CHANGING data = lt_selopts ).

  "--- Build standard RSPARAMS table ---
  LOOP AT lt_selopts INTO ls_selopt.
    CLEAR ls_rsparam.
    ls_rsparam-selname = to_upper( ls_selopt-selname ).
    ls_rsparam-kind    = COND #( WHEN ls_selopt-kind   IS INITIAL THEN 'P' ELSE to_upper( ls_selopt-kind ) ).
    ls_rsparam-sign    = COND #( WHEN ls_selopt-sign   IS INITIAL THEN 'I' ELSE to_upper( ls_selopt-sign ) ).
    ls_rsparam-option  = COND #( WHEN ls_selopt-option IS INITIAL THEN 'EQ' ELSE to_upper( ls_selopt-option ) ).
    ls_rsparam-low     = ls_selopt-low.
    ls_rsparam-high    = ls_selopt-high.
    APPEND ls_rsparam TO lt_rsparams.
  ENDLOOP.

  "--- Inject TESTLAUF / SIMULATION flag if test run ---
  IF iv_test_run = abap_true.
    READ TABLE lt_rsparams TRANSPORTING NO FIELDS WITH KEY selname = 'TESTLAUF'.
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


  "============================================================
  "--- Submit as a background job (ALWAYS — never inline SUBMIT) ---
  "============================================================

  "--- Generate unique job name (max 32 chars) ---
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
    ev_status  = lc_error.
    lv_msg = 'Cannot create background job — check S_BTCH_ADM auth'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Submit report step to the open job ---
  SUBMIT (iv_progname)
    WITH SELECTION-TABLE lt_rsparams
    VIA JOB     lv_jobname
    NUMBER      lv_jobcount
    AND RETURN.

  IF sy-subrc <> 0.
    ev_status  = lc_error.
    lv_msg = 'Cannot attach program ' && iv_progname && ' to job'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
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
    ev_status  = lc_error.
    lv_msg = 'Cannot release job ' && lv_jobname.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    RETURN.
  ENDIF.

  "============================================================
  "--- IV_ASYNC = 'X' : return immediately; caller polls ---
  "============================================================
  IF iv_async = abap_true.
    ev_status  = lc_async.  "Async — job submitted, result pending
    ev_result_json = '{"status":"submitted"' &&
                     ',"mode":"async"' &&
                     ',"program":"'  && iv_progname && '"' &&
                     ',"jobname":"'  && lv_jobname  && '"' &&
                     ',"jobcount":"' && lv_jobcount && '"}'.
    lv_msg = 'Job submitted: ' && lv_jobname && ' / ' && lv_jobcount.
    PERFORM z_add_msg USING lc_success lv_msg CHANGING et_messages.
    RETURN.
  ENDIF.

  "============================================================
  "--- IV_ASYNC = '' : wait inline for the job, return spool ---
  "============================================================
  lv_waited = 0.
  DO.
    PERFORM z_get_jobstate USING lv_jobname lv_jobcount CHANGING lv_state.
    IF lv_state = 'FINISHED' OR lv_state = 'ABORTED' OR lv_state = 'NOTFOUND'.
      EXIT.
    ENDIF.
    IF lv_waited >= lc_sync_wait_timeout.
      EXIT.
    ENDIF.
    WAIT UP TO lc_sync_wait_interval SECONDS.
    lv_waited = lv_waited + lc_sync_wait_interval.
  ENDDO.

  IF lv_state <> 'FINISHED'.
    ev_status = lc_error.
    ev_result_json = '{"status":"error"' &&
                     ',"mode":"sync_wait"' &&
                     ',"program":"'  && iv_progname && '"' &&
                     ',"jobname":"'  && lv_jobname  && '"' &&
                     ',"jobcount":"' && lv_jobcount && '"' &&
                     ',"state":"'    && lv_state    && '"}'.
    lv_msg = 'Job ' && lv_jobname && ' did not finish (state=' && lv_state && ')'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Job finished: fetch spool and embed it in the result ---
  PERFORM z_fetch_spool USING    lv_jobname lv_jobcount
                        CHANGING lt_lines lv_subrc lv_spoolmsg.

  TRY.
    lv_spooljson = /ui2/cl_json=>serialize( data = lt_lines ).
  CATCH cx_root.
    lv_spooljson = '[]'.
  ENDTRY.

  ev_status = lc_success.
  ev_result_json = '{"status":"completed"' &&
                   ',"mode":"sync_wait"' &&
                   ',"program":"'  && iv_progname && '"' &&
                   ',"jobname":"'  && lv_jobname  && '"' &&
                   ',"jobcount":"' && lv_jobcount && '"' &&
                   ',"spool":'     && lv_spooljson && '}'.

  IF lv_subrc <> 0.
    "Spool unavailable is not fatal — the job itself finished OK.
    lv_msg = 'Job finished, spool unavailable: ' && lv_spoolmsg.
    PERFORM z_add_msg USING lc_warning lv_msg CHANGING et_messages.
  ELSE.
    lv_msg = 'Program ' && iv_progname && ' completed (inline-wait); spool lines=' && lines( lt_lines ).
    PERFORM z_add_msg USING lc_success lv_msg CHANGING et_messages.
  ENDIF.

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

FORM z_execute_bdc
  USING    iv_tcode       TYPE sy-repid
           iv_params_json TYPE string
           iv_test_run    TYPE abap_bool
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: lt_screens TYPE ty_zai_bdc_screen_tab,
        ls_screen  TYPE ty_zai_bdc_screen,
        lt_bdcdata TYPE TABLE OF bdcdata,
        ls_bdcdata TYPE bdcdata,
        lt_messtab TYPE TABLE OF bdcmsgcoll,
        ls_messtab TYPE bdcmsgcoll,
        lv_tcode   TYPE tcode,
        lv_mode    TYPE c LENGTH 1,
        lv_prog    TYPE sy-repid,
        lv_dynr    TYPE sy-dynnr,
        ls_bapiret TYPE bapiret2,
        lv_msg     TYPE string,
        lv_message_count TYPE i.

  "--- Parse JSON screen/field definition ---
  /ui2/cl_json=>deserialize(
    EXPORTING json = iv_params_json
    CHANGING  data = lt_screens ).

  IF lt_screens IS INITIAL.
    ev_status  = lc_error.
    PERFORM z_add_msg USING ev_status 'BDC screen data is empty — check IV_PARAMS_JSON' CHANGING et_messages.
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
  READ TABLE et_messages TRANSPORTING NO FIELDS WITH KEY type = lc_error.
  IF sy-subrc = 0.
    ev_status  = lc_error.
    lv_msg = 'BDC ' && iv_tcode && ' completed with errors'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
  ELSE.
    ev_status  = lc_success.
    lv_msg = 'BDC ' && iv_tcode && ' completed successfully'.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
  ENDIF.

  lv_message_count = lines( et_messages ).
  ev_result_json = '{"status":"' && ev_status && '"' &&
                   ',"tcode":"' && iv_tcode && '"' &&
                   ',"message_count":"' && lv_message_count && '"}'.

ENDFORM.


FORM z_execute_tool
  USING    iv_toolname    TYPE sy-repid
           iv_params_json TYPE string
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

   CASE iv_toolname.

      WHEN 'TOOL_READ_TABLE'.
        PERFORM ztool_read_table
          USING    iv_params_json
          CHANGING ev_status
                   ev_result_json
                   et_messages.

      WHEN 'TOOL_JOB_STATUS'.
        PERFORM ztool_job_status
          USING    iv_params_json
          CHANGING ev_status
                   ev_result_json
                   et_messages.

      WHEN 'TOOL_READ_JOB_SPOOL'.
        PERFORM ztool_read_job_spool
          USING    iv_params_json
          CHANGING ev_status
                   ev_result_json
                   et_messages.

      WHEN OTHERS.
        ev_status = lc_error.
        DATA(lv_msg) = 'Unknown TOOL: ' && iv_toolname.
        PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.

   ENDCASE.

ENDFORM.


*&============================================================*
*& HANDLER 4: READ_TABLE — Direct Table Read
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
       "table":      "BSEG"
*      "where":      "KOKRS EQ '1000' AND GJAHR EQ '2024' AND PERAB EQ '012'",
*      "fields":     "KOKRS,GJAHR,PERAB,KSTAR,WKGBTR",
*      "max_rows":   50
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

FORM ztool_read_table
  USING    iv_params_json TYPE string
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
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
        lv_fsep     TYPE string,
        lv_msg      TYPE string.

  "--- Parse query params ---
  /ui2/cl_json=>deserialize(
    EXPORTING json = iv_params_json
    CHANGING  data = ls_params ).

  IF ls_params-max_rows = 0.
    ls_params-max_rows = 1000.
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
      query_table          = ls_params-table
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
      ev_status = lc_success.
    WHEN 1.
      ev_status  = lc_error.
      lv_msg = 'Table not available: ' && ls_params-table.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
      RETURN.
    WHEN 2.
      ev_status  = lc_warning.
      lv_msg = 'Table ' && ls_params-table && ' is empty for given criteria'.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
      ev_result_json = '{"table":"' && ls_params-table &&
                       '","rows":[],"count":0}'.
      RETURN.
    WHEN OTHERS.
      ev_status  = lc_error.
      lv_msg = 'RFC_READ_TABLE error on ' && ls_params-table && ', subrc=' && sy-subrc.
      PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
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

  lv_msg  = 'Read ' && lv_cnt && ' row(s) from ' && ls_params-table.
  PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.

  ev_result_json = '{"table":"' && ls_params-table && '"' &&
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
*&============================================================*

FORM ztool_job_status
  USING    iv_params_json TYPE string
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: ls_job   TYPE ty_zai_job_params,
        lv_state TYPE string,
        lv_msg   TYPE string.

  "--- Deserialize JSON ---
  /ui2/cl_json=>deserialize(
     EXPORTING json = iv_params_json
     CHANGING  data = ls_job ).

  "--- Query job status (shared helper) ---
  PERFORM z_get_jobstate USING ls_job-jobname ls_job-jobcount CHANGING lv_state.

  IF lv_state = 'NOTFOUND'.
    ev_status  = lc_error.
    lv_msg = 'Job not found: ' && ls_job-jobname && ' / ' && ls_job-jobcount.
    PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.
    RETURN.
  ENDIF.

  "--- Map state to a single status ---
  CASE lv_state.
    WHEN 'FINISHED'. ev_status = lc_success.
    WHEN 'ABORTED'.  ev_status = lc_error.
    WHEN OTHERS.     ev_status = lc_async.   "RUNNING | READY | SCHEDULED
  ENDCASE.

  lv_msg = 'Job ' && ls_job-jobname && ': ' && lv_state.
  PERFORM z_add_msg USING ev_status lv_msg CHANGING et_messages.

  ev_result_json = '{"jobname":"'  && ls_job-jobname  && '"' &&
                   ',"jobcount":"' && ls_job-jobcount && '"' &&
                   ',"state":"'    && lv_state        && '"' &&
                   ',"status":"'   && ev_status       && '"}'.

ENDFORM.


FORM ztool_read_job_spool
  USING    iv_params_json TYPE string
  CHANGING ev_status      TYPE c
           ev_result_json TYPE string
           et_messages    TYPE bapiret2_t.

  DATA: ls_job     TYPE ty_zai_job_params,
        lv_message TYPE string,
        lv_subrc   TYPE i,
        lt_lines   TYPE stringtab.

  "------------------------------------------------------------
  " 1. Deserialize input JSON
  "------------------------------------------------------------
  TRY.
    /ui2/cl_json=>deserialize( EXPORTING json = iv_params_json
                               CHANGING data = ls_job ).
  CATCH cx_root INTO DATA(lx_json).
    ev_status  = lc_error.
    lv_message = 'JSON deserialize error: ' && lx_json->get_text( ).
    PERFORM z_add_msg USING ev_status lv_message CHANGING et_messages.
    RETURN.
  ENDTRY.

  "------------------------------------------------------------
  " 2. Resolve + read spool (shared helper)
  "------------------------------------------------------------
  PERFORM z_fetch_spool USING    ls_job-jobname ls_job-jobcount
                        CHANGING lt_lines lv_subrc lv_message.

  IF lv_subrc <> 0.
    ev_status  = lc_error.
    PERFORM z_add_msg USING ev_status lv_message CHANGING et_messages.
    RETURN.
  ENDIF.

  "------------------------------------------------------------
  " 3. Serialise result to JSON (array of lines)
  "------------------------------------------------------------
  TRY.
    ev_result_json = /ui2/cl_json=>serialize( data = lt_lines ).
  CATCH cx_root INTO DATA(lx_json_ser).
    ev_status  = lc_error.
    lv_message = |JSON serialize error: { lx_json_ser->get_text( ) }|.
    PERFORM z_add_msg USING ev_status lv_message CHANGING et_messages.
    RETURN.
  ENDTRY.

  ev_status  = lc_success.
  lv_message = |Spool read successfully. Lines={ lines( lt_lines ) }|.
  PERFORM z_add_msg USING ev_status lv_message CHANGING et_messages.

ENDFORM.


*&--------------------------------------------------------------------*
*& FORM z_get_jobstate
*&   Wraps SHOW_JOBSTATE and maps the flags to a single state string:
*&   FINISHED | ABORTED | RUNNING | READY | SCHEDULED | NOTFOUND
*&--------------------------------------------------------------------*
FORM z_get_jobstate
  USING    iv_jobname  TYPE btcjob
           iv_jobcount TYPE btcjobcnt
  CHANGING ev_state    TYPE string.

  DATA: lv_aborted TYPE c LENGTH 1,
        lv_finish  TYPE c LENGTH 1,
        lv_running TYPE c LENGTH 1,
        lv_ready   TYPE c LENGTH 1,
        lv_schedul TYPE c LENGTH 1.

  CLEAR ev_state.

  CALL FUNCTION 'SHOW_JOBSTATE'
    EXPORTING
      jobcount         = iv_jobcount
      jobname          = iv_jobname
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
    ev_state = 'NOTFOUND'.
    RETURN.
  ENDIF.

  IF lv_finish = 'X'.
    ev_state = 'FINISHED'.
  ELSEIF lv_aborted = 'X'.
    ev_state = 'ABORTED'.
  ELSEIF lv_running = 'X'.
    ev_state = 'RUNNING'.
  ELSEIF lv_ready = 'X'.
    ev_state = 'READY'.
  ELSE.
    ev_state = 'SCHEDULED'.
  ENDIF.

ENDFORM.


*&--------------------------------------------------------------------*
*& FORM z_fetch_spool
*&   Resolves the spool request for a (finished) job via TBTCP→RQIDENT
*&   and returns its lines. ev_subrc = 0 on success; on error ev_subrc
*&   is non-zero and ev_message carries the explanation.
*&--------------------------------------------------------------------*
FORM z_fetch_spool
  USING    iv_jobname  TYPE btcjob
           iv_jobcount TYPE btcjobcnt
  CHANGING ct_lines    TYPE stringtab
           ev_subrc    TYPE i
           ev_message  TYPE string.

  DATA: lt_buffer    TYPE TABLE OF buffer,
        lv_rqident   TYPE tsp01-rqident,
        lv_listident TYPE tbtcp-listident,
        lv_line      TYPE string.

  CLEAR: ct_lines, ev_message.
  ev_subrc = 0.

  "--- Get RQIDENT by JOBNAME / JOBCOUNT ---
  SELECT SINGLE listident
    INTO lv_listident
    FROM tbtcp
   WHERE jobname  = iv_jobname
     AND jobcount = iv_jobcount.

  lv_rqident = lv_listident.

  IF sy-subrc <> 0 OR lv_rqident IS INITIAL.
    ev_subrc   = 4.
    ev_message = |Spool not found for jobname={ iv_jobname } jobcount={ iv_jobcount }|.
    RETURN.
  ENDIF.

  "--- Read spool ---
  CALL FUNCTION 'RSPO_RETURN_ABAP_SPOOLJOB'
    EXPORTING
      rqident              = lv_rqident
    TABLES
      buffer               = lt_buffer
    EXCEPTIONS
      no_such_job          = 1
      job_contains_no_data = 2
      selection_empty      = 3
      no_permission        = 4
      can_not_access       = 5
      read_error           = 6
      type_no_match        = 7
      OTHERS               = 8.

  IF sy-subrc <> 0.
    ev_subrc = sy-subrc.
    CASE sy-subrc.
      WHEN 1. ev_message = |No such spool job. rqident={ lv_rqident }|.
      WHEN 2. ev_message = |Spool job contains no data. rqident={ lv_rqident }|.
      WHEN 3. ev_message = |Selection empty. rqident={ lv_rqident }|.
      WHEN 4. ev_message = |No permission to read spool. rqident={ lv_rqident }|.
      WHEN 5. ev_message = |Can not access spool. rqident={ lv_rqident }|.
      WHEN 6. ev_message = |Read error spool. rqident={ lv_rqident }|.
      WHEN 7. ev_message = |Type no match while reading spool. rqident={ lv_rqident }|.
      WHEN OTHERS. ev_message = |Unexpected error while reading spool. rqident={ lv_rqident }|.
    ENDCASE.
    RETURN.
  ENDIF.

  "--- Convert CHAR(255) buffer rows to string lines ---
  LOOP AT lt_buffer INTO DATA(lv_buf).
    lv_line = lv_buf.
    APPEND lv_line TO ct_lines.
  ENDLOOP.

ENDFORM.


*&============================================================*
*& HELPERS
*&============================================================*

*&--------------------------------------------------------------------*
*& FORM z_parse_flat_json
*&  Parses a simple flat JSON object {"K1":"V1","K2":"V2",...}
*&  into an internal name-value table.
*&  Note: Works for scalar string values only.
*&        For nested structures use /ui2/cl_json=>deserialize directly.
*&--------------------------------------------------------------------*
FORM z_parse_flat_json
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
*& FORM z_build_json
*&  Serializes a name-value table to a flat JSON object string.
*&--------------------------------------------------------------------*
FORM z_build_json
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
*& FORM z_add_msg
*&  Appends a simple message to ET_MESSAGES (BAPIRET2).
*&--------------------------------------------------------------------*
FORM z_add_msg USING    iv_type     TYPE c
                        iv_text     TYPE string
               CHANGING et_messages TYPE bapiret2_t.

  DATA: ls_msg TYPE bapiret2.
  ls_msg-type       = iv_type.
  ls_msg-id         = 'ZAI_PERIOD'.
  ls_msg-number     = '001'.
  ls_msg-message    = iv_text.
  ls_msg-message_v1 = iv_text.
  APPEND ls_msg TO et_messages.

ENDFORM.