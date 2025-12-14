#!/usr/bin/env python3
"""
Script per estrarre deal da HubSpot e inserirli in Google Sheets.
Esegue automaticamente alle 05:05 se usato con --schedule
"""

import requests
from datetime import datetime
import os
import sys
import time
import schedule
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Carica variabili d'ambiente
load_dotenv()

# Configurazione da variabili d'ambiente
HUBSPOT_API_TOKEN = os.getenv("HUBSPOT_API_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1JtvLP9vLPkn98seLav0tUQShvQyICSfLA87eP-cv7uk")

# Partner da filtrare con i rispettivi nomi dei fogli
PARTNERS = {
    "Smallpay": "SmallPay",
    "Deutsche Bank": "Deutsche Bank",
    "Attitude": "Attitude",
    "PostePay": "PostePay"
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HUBSPOT_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
    "Content-Type": "application/json"
}

# Pipeline Partnership (unica da considerare)
PARTNERSHIP_PIPELINE_ID = "1347411134"

# Stage IDs per la pipeline Partnership
PIPELINES = {
    "partnership": {
        "pipeline_id": "1347411134",
        "proposal_sent": "1834011865",
        "kyc_pending_approval": "1834011866",
        "onboarding_completed": "2019816637"
    }
}

# Tutti gli stage IDs per tutte le pipeline
ALL_PROPOSAL_SENT_IDS = [p["proposal_sent"] for p in PIPELINES.values()]
ALL_KYC_IDS = [p["kyc_pending_approval"] for p in PIPELINES.values()]
ALL_ONBOARDING_IDS = [p["onboarding_completed"] for p in PIPELINES.values()]

# Proprietà HubSpot da recuperare (tutte le combinazioni di stage)
HUBSPOT_PROPERTIES = [
    "dealname", "createdate", "amount", "dealstage", "pipeline",
    "partner_label_name", "ttv_all_time", "instore_category", "offline_annual_revenue",
    "first_order_ttv", "days_between_create_and_kyc",
    # Nuove colonne comuni
    "risk_check_status", "store_type",
    # Colonne per Attitude
    "third_party___customer_tier", "third_party___remuneration",
    "original_agent_source_name", "third_party___fixed_fee", "third_party___products__fee",
    # Colonne per Deutsche Bank
    "original_agent_email",
]
# Aggiungi proprietà V2 per ogni stage ID di ogni pipeline (V2 ha i dati reali)
for stage_id in ALL_KYC_IDS:
    HUBSPOT_PROPERTIES.append(f"hs_v2_date_entered_{stage_id}")
for stage_id in ALL_ONBOARDING_IDS:
    HUBSPOT_PROPERTIES.append(f"hs_v2_date_entered_{stage_id}")
for stage_id in ALL_PROPOSAL_SENT_IDS:
    HUBSPOT_PROPERTIES.append(f"hs_v2_date_entered_{stage_id}")
    HUBSPOT_PROPERTIES.append(f"hs_v2_date_exited_{stage_id}")
    HUBSPOT_PROPERTIES.append(f"hs_v2_cumulative_time_in_{stage_id}")

# Header base (comuni a tutti)
BASE_HEADERS = [
    "Deal ID", "Deal name", "Deal Create date", "Deal Amount", "Deal stage",
    "Partner Name", "Deal TTV All Time", "Deal InStore Category",
    "Deal Date entered \"KYC Pending Approval\"",
    "Deal Date entered \"Onboarding Completed\"",
    "Deal Offline Annual Revenue", "Deal First Order TTV",
    "Deal Days between Create and KYC (min)",
    "Deal Date entered \"Proposal sent\"",
    "Deal Date exited \"Proposal sent\"",
    "Deal Cumulative time in \"Proposal sent\" (min)",
    "Ore in Proposal sent",
    # Nuove colonne comuni
    "Risk Check Status", "Store Type", "Deal Size"
]

# Header aggiuntivi per partner specifici
ATTITUDE_EXTRA_HEADERS = [
    "Third Party - Customer Tier", "Third Party - Remuneration",
    "Original Agent Source Name", "Third Party - Fixed Fee", "Third Party - Products Fee"
]

DEUTSCHE_BANK_EXTRA_HEADERS = [
    "Original Agent Email", "Third Party - Customer Tier", "Third Party - Products Fee"
]

# Funzione per ottenere headers per partner
def get_headers_for_partner(partner_keyword):
    headers = BASE_HEADERS.copy()
    if partner_keyword == "Attitude":
        headers.extend(ATTITUDE_EXTRA_HEADERS)
    elif partner_keyword == "Deutsche Bank":
        headers.extend(DEUTSCHE_BANK_EXTRA_HEADERS)
    return headers

# Labels globali
STAGE_LABELS = {}
INSTORE_CATEGORY_LABELS = {}


def get_google_sheets_service():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(script_dir, "token.json")
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds.valid and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


def load_stage_labels():
    """Carica le label degli stage delle pipeline."""
    global STAGE_LABELS
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    response = requests.get(url, headers=HUBSPOT_HEADERS)
    for pipeline in response.json().get("results", []):
        for stage in pipeline.get("stages", []):
            STAGE_LABELS[stage["id"]] = stage["label"]


def load_instore_category_labels():
    """Carica le label per instore_category da HubSpot."""
    global INSTORE_CATEGORY_LABELS
    url = "https://api.hubapi.com/crm/v3/properties/deals/instore_category"
    response = requests.get(url, headers=HUBSPOT_HEADERS)
    data = response.json()
    for opt in data.get("options", []):
        INSTORE_CATEGORY_LABELS[opt["value"]] = opt["label"]


def get_all_deals():
    """Recupera solo i deal dalla Partnership Pipeline usando Search API."""
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    all_deals = []
    after = 0

    while True:
        # Usa Search API con filtro per pipeline
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": PARTNERSHIP_PIPELINE_ID
                }]
            }],
            "properties": HUBSPOT_PROPERTIES,
            "limit": 100,
            "after": after
        }

        response = requests.post(url, headers=HUBSPOT_HEADERS, json=payload)
        data = response.json()

        results = data.get("results", [])
        all_deals.extend(results)

        # Paging per Search API
        paging = data.get("paging", {})
        next_page = paging.get("next", {})
        after = next_page.get("after")

        if not after or len(results) == 0:
            break

        print(f"  Recuperati {len(all_deals)} deal dalla Partnership Pipeline...", flush=True)

    return all_deals


def parse_date(date_string):
    """Parse una stringa data e ritorna oggetto datetime o None."""
    if not date_string:
        return None
    try:
        return datetime.fromisoformat(date_string.replace("Z", "+00:00"))
    except:
        return None


def format_date(date_string):
    """Formatta data come stringa YYYY-MM-DD HH:MM:SS."""
    if not date_string:
        return ""
    try:
        dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return date_string


def format_euro(value):
    """Converte valore in numero per formato Euro."""
    if not value:
        return ""
    try:
        return float(str(value).replace(",", ".").replace(" ", ""))
    except:
        return value


def classify_deal_size(value, store_type=""):
    """Classifica il deal in base all'amount. Se Physical store, usa amount/0.05 per il clustering."""
    if not value:
        return ""
    try:
        amount = float(str(value).replace(",", ".").replace(" ", ""))
        # Se Physical store, usa amount/0.05 per il clustering (senza salvare)
        if store_type and "physical store" in store_type.lower():
            amount = amount / 0.05
        if amount < 50000:
            return "0 - 50.000 €"
        elif amount < 100000:
            return "50.000 € - 100.000 €"
        elif amount < 300000:
            return "100.000 € - 300.000 €"
        elif amount < 500000:
            return "300.000 € - 500.000 €"
        elif amount < 1000000:
            return "500.000 € - 1M €"
        elif amount < 5000000:
            return "1M € - 5M €"
        elif amount < 10000000:
            return "5M € - 10M €"
        else:
            return "Oltre 10M €"
    except:
        return ""


def format_ms_to_minutes(ms_string):
    """Converte millisecondi in minuti."""
    if not ms_string:
        return ""
    try:
        ms = float(str(ms_string).replace(",", ".").replace(" ", ""))
        return round(ms / 1000 / 60, 2)
    except:
        return ms_string


def calculate_hours_in_proposal(date_entered, date_exited, stage_label):
    """
    Calcola ore in Proposal sent:
    - Se date_entered e date_exited presenti: diff in ore
    - Se solo date_entered e stage = "Proposal sent": ore da entered a ora
    - Altrimenti: vuoto
    """
    entered_dt = parse_date(date_entered)
    exited_dt = parse_date(date_exited)

    if not entered_dt:
        return ""

    # Se abbiamo entrambe le date, calcoliamo la differenza
    if exited_dt:
        diff_hours = (exited_dt - entered_dt).total_seconds() / 3600
        return round(diff_hours, 2)

    # Se stage è "Proposal sent" (o simile), calcola da entered a ora
    if stage_label and "proposal sent" in stage_label.lower():
        now = datetime.now(entered_dt.tzinfo) if entered_dt.tzinfo else datetime.now()
        diff_hours = (now - entered_dt).total_seconds() / 3600
        return round(diff_hours, 2)

    return ""


def get_first_value(props, stage_ids, prefix="hs_v2_date_entered_"):
    """Ritorna il primo valore non vuoto tra gli stage IDs (usa proprietà V2)."""
    for stage_id in stage_ids:
        value = props.get(f"{prefix}{stage_id}", "")
        if value:
            return value
    return ""


def process_deals(deals, partner_keyword=""):
    """Processa i deal e ritorna righe formattate."""
    rows = []
    for deal in deals:
        props = deal.get("properties", {})
        stage_id = props.get("dealstage", "")
        stage_label = STAGE_LABELS.get(stage_id, stage_id)

        # Cerca valori in tutte le pipeline (prende il primo non vuoto) - usa proprietà V2
        date_entered_kyc = get_first_value(props, ALL_KYC_IDS, "hs_v2_date_entered_")
        date_entered_onboarding = get_first_value(props, ALL_ONBOARDING_IDS, "hs_v2_date_entered_")
        date_entered_proposal = get_first_value(props, ALL_PROPOSAL_SENT_IDS, "hs_v2_date_entered_")
        date_exited_proposal = get_first_value(props, ALL_PROPOSAL_SENT_IDS, "hs_v2_date_exited_")
        time_in_proposal = get_first_value(props, ALL_PROPOSAL_SENT_IDS, "hs_v2_cumulative_time_in_")

        # Calcola ore in proposal
        hours_in_proposal = calculate_hours_in_proposal(
            date_entered_proposal,
            date_exited_proposal,
            stage_label
        )

        # InStore category con label
        instore_value = props.get("instore_category", "")
        instore_label = INSTORE_CATEGORY_LABELS.get(instore_value, instore_value)

        # Riga base (comuni a tutti)
        row = [
            deal.get("id", ""),
            props.get("dealname", ""),
            format_date(props.get("createdate", "")),
            format_euro(props.get("amount", "")),                    # D: Euro
            stage_label,                                              # E: Stage
            props.get("partner_label_name", ""),
            format_euro(props.get("ttv_all_time", "")),              # G: Euro
            instore_label,                                            # H: Label
            format_date(date_entered_kyc),                            # I: KYC
            format_date(date_entered_onboarding),                     # J: Onboarding
            format_euro(props.get("offline_annual_revenue", "")),
            format_euro(props.get("first_order_ttv", "")),
            format_ms_to_minutes(props.get("days_between_create_and_kyc", "")),  # M: Minuti
            format_date(date_entered_proposal),                       # N: Date entered
            format_date(date_exited_proposal),                        # O: Date exited
            format_ms_to_minutes(time_in_proposal),                   # P: Minuti
            hours_in_proposal,                                        # Q: Ore calcolate
            # Nuove colonne comuni
            props.get("risk_check_status", ""),
            props.get("store_type", ""),
            classify_deal_size(props.get("amount", ""), props.get("store_type", ""))  # Deal Size
        ]

        # Colonne aggiuntive per Attitude
        if partner_keyword == "Attitude":
            row.extend([
                props.get("third_party___customer_tier", ""),
                props.get("third_party___remuneration", ""),
                props.get("original_agent_source_name", ""),
                props.get("third_party___fixed_fee", ""),
                props.get("third_party___products__fee", "")
            ])
        # Colonne aggiuntive per Deutsche Bank
        elif partner_keyword == "Deutsche Bank":
            row.extend([
                props.get("original_agent_email", ""),
                props.get("third_party___customer_tier", ""),
                props.get("third_party___products__fee", "")
            ])

        rows.append(row)
    return rows


def filter_deals_by_partner(deals, partner_keyword):
    """Filtra i deal in base al partner_label_name e pipeline Partnership."""
    filtered = []
    for deal in deals:
        props = deal.get("properties", {})
        partner_name = props.get("partner_label_name", "") or ""
        pipeline = props.get("pipeline", "")
        # Filtra solo pipeline Partnership e partner name corrispondente
        if pipeline == PARTNERSHIP_PIPELINE_ID and partner_keyword.lower() in partner_name.lower():
            filtered.append(deal)
    return filtered


def ensure_sheet_exists(service, sheet_name):
    """Crea il foglio se non esiste."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
        existing_sheets = [s["properties"]["title"] for s in spreadsheet.get("sheets", [])]
        if sheet_name not in existing_sheets:
            request = {
                "requests": [{
                    "addSheet": {
                        "properties": {"title": sheet_name}
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body=request
            ).execute()
            print(f"    Creato foglio '{sheet_name}'", flush=True)
    except Exception as e:
        print(f"    Errore creazione foglio: {e}", flush=True)


def clear_sheet(service, sheet_name):
    """Pulisce il contenuto del foglio."""
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"'{sheet_name}'!A:Z"
        ).execute()
    except:
        pass


def get_sheet_id(service, sheet_name):
    """Ottiene l'ID del foglio dal nome."""
    spreadsheet = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    return None


def format_sheet(service, sheet_name, num_rows):
    """Applica formattazione Euro e numero alle colonne."""
    sheet_id = get_sheet_id(service, sheet_name)
    if not sheet_id:
        return

    requests_list = []

    # Colonne Euro: D (index 3), G (index 6), K (index 10), L (index 11)
    euro_columns = [3, 6, 10, 11]
    for col_idx in euro_columns:
        requests_list.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": num_rows + 1,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "NUMBER",
                            "pattern": '#,##0.00"€"'
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    # Colonna minuti M (index 12) - Days between Create and KYC
    requests_list.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": 12,
                "endColumnIndex": 13
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "0.00"
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    # Colonna minuti P (index 15) - Cumulative time in Proposal sent
    requests_list.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": 15,
                "endColumnIndex": 16
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "0.00"
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    # Colonna ore Q (index 16) - Ore in Proposal sent
    requests_list.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": 16,
                "endColumnIndex": 17
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": '0.00"h"'
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    if requests_list:
        service.spreadsheets().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={"requests": requests_list}
        ).execute()


def write_to_sheets(service, rows, sheet_name, partner_keyword):
    headers = get_headers_for_partner(partner_keyword)
    data = [headers] + rows
    result = service.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": data}
    ).execute()
    return result


def run_export():
    """Esegue l'export completo."""
    print("=" * 50, flush=True)
    print(f"HubSpot to Google Sheets - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 50, flush=True)

    print("\n[1/5] Caricamento stage...", flush=True)
    load_stage_labels()
    print(f"  {len(STAGE_LABELS)} stage caricati", flush=True)

    print("\n[2/5] Caricamento categorie InStore...", flush=True)
    load_instore_category_labels()
    print(f"  {len(INSTORE_CATEGORY_LABELS)} categorie caricate", flush=True)

    print("\n[3/5] Recupero deal dalla Partnership Pipeline...", flush=True)
    deals = get_all_deals()
    print(f"  {len(deals)} deal trovati nella Partnership Pipeline", flush=True)

    print("\n[4/5] Connessione a Google Sheets...", flush=True)
    service = get_google_sheets_service()
    print("  Connesso!", flush=True)

    print("\n[5/5] Export per partner...", flush=True)
    total_cells = 0
    for partner_keyword, sheet_name in PARTNERS.items():
        print(f"\n  [{partner_keyword}]", flush=True)

        # Filtra deal per partner
        partner_deals = filter_deals_by_partner(deals, partner_keyword)
        print(f"    {len(partner_deals)} deal trovati", flush=True)

        if len(partner_deals) == 0:
            print(f"    Nessun deal per {partner_keyword}, skip.", flush=True)
            continue

        # Processa i deal con colonne specifiche per partner
        rows = process_deals(partner_deals, partner_keyword)

        # Crea foglio se non esiste
        ensure_sheet_exists(service, sheet_name)

        # Pulisci foglio esistente
        clear_sheet(service, sheet_name)

        # Scrivi dati con header specifici per partner
        result = write_to_sheets(service, rows, sheet_name, partner_keyword)
        cells = result.get('updatedCells', 0)
        total_cells += cells
        print(f"    {cells} celle scritte su '{sheet_name}'", flush=True)

        # Applica formattazione
        format_sheet(service, sheet_name, len(rows))
        print(f"    Formattazione applicata", flush=True)

    print("\n" + "=" * 50, flush=True)
    print(f"FATTO! {total_cells} celle totali aggiornate", flush=True)
    print(f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}", flush=True)
    print("=" * 50, flush=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        print("Modalità schedulata attiva - Export giornaliero alle 05:05", flush=True)
        print("Premi Ctrl+C per uscire\n", flush=True)

        # Esegui subito la prima volta
        run_export()

        # Schedula per le 05:05 ogni giorno
        schedule.every().day.at("05:05").do(run_export)

        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Esecuzione singola
        run_export()


if __name__ == "__main__":
    main()
