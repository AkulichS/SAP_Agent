*&============================================================*
*&    TOP INCLUDE (LZPAI_PERIODTOP)
*&============================================================*

FUNCTION-POOL ZFI_AI_PERIOD_CLOSE.          "MESSAGE-ID ..
* INCLUDE LZFI_AI_PERIOD_CLOSED...           " Local class definition

TYPES:

  "--- Simple key-value pair (used by FM handler) ---
  BEGIN OF ty_zai_kv,
    name  TYPE string,
    value TYPE string,
  END OF ty_zai_kv,
  ty_zai_kv_tab TYPE STANDARD TABLE OF ty_zai_kv WITH DEFAULT KEY,

  "--- Selection parameter for SUBMIT handler ---
  "    Mirrors RSPARAMS but with string LOW/HIGH for JSON flexibility
  ty_zai_selparam_tab TYPE STANDARD TABLE OF RSPARAMS WITH DEFAULT KEY,

  "--- BDC field within a screen ---
  BEGIN OF ty_zai_bdc_field,
    fnam TYPE string,      "Field name on screen
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
    table    TYPE string,
    where    TYPE string,  "WHERE clause e.g. "BUKRS EQ '1000' AND GJAHR EQ '2024'"
    fields   TYPE string,  "Comma-separated field list e.g. "KOKRS,GJAHR,LFMON"
    max_rows TYPE int4,    "Row limit (default 100)
  END OF ty_zai_check_params,

  "--- FM/BAPI call params ---
  BEGIN OF ty_zai_fm_params,
    commit_work TYPE abap_bool,  "X = call BAPI_TRANSACTION_COMMIT after FM
  END OF ty_zai_fm_params.