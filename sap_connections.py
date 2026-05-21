from pyrfc import Connection

# Parameters for connecting to the SAP system
def sap_params():
    return {
        'ashost': 's40ap01.torg.x5.ru',                   # SAP server address
        'sysnr': '00',                                    # SAP system number
        'client': '150',                                  # SAP client number
        'snc_mode': '1',                                  # enabling SNC (1 - is enabled)
        'snc_myname': 'p:CN=SERGEY.AKULICH@X5.RU, C=EN',  # user SNC-name
        'snc_partnername': 'p:CN=SRV.SSO-ABAP-S40@x5.ru', # SNC-name of SAP server
        'lang': 'EN'
    }

def connect_to_sap():
    try:  
        conn =  Connection(**sap_params())
        return conn
    except Exception as e:
        print("Error in connection:", e)
        return None
    