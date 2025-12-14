# B2B Partner Tracking - HubSpot to Google Sheets Export

Script Python per esportare automaticamente i deal HubSpot su Google Sheets, filtrati per partner e pipeline.

## Funzionalità

- **Export automatico** da HubSpot CRM a Google Sheets
- **Filtro Pipeline**: Solo deal dalla "Partnership - Pipeline"
- **Filtro Partner**: 4 fogli separati per partner
  - SmallPay
  - Deutsche Bank
  - Attitude
  - PostePay
- **Colonne partner-specifiche**:
  - **Attitude**: Customer Tier, Remuneration, Agent Source, Fixed Fee, Products Fee
  - **Deutsche Bank**: Agent Email, Customer Tier, Products Fee
- **Scheduling**: Esecuzione giornaliera alle 05:05 CET via GitHub Actions
- **Formattazione**: Euro per Amount/TTV, ore per tempo in stage

## Colonne Esportate

| Colonna | Descrizione |
|---------|-------------|
| Deal ID | ID univoco del deal |
| Deal name | Nome del deal |
| Deal Create date | Data creazione |
| Deal Amount | Importo (formato Euro) |
| Deal stage | Stage attuale |
| Partner Name | Nome del partner |
| Deal TTV All Time | Total Transaction Value (formato Euro) |
| Deal InStore Category | Categoria InStore |
| Deal Date entered "KYC Pending Approval" | Data ingresso KYC |
| Deal Date entered "Onboarding Completed" | Data completamento onboarding |
| Deal Offline Annual Revenue | Revenue annuale offline |
| Deal First Order TTV | TTV primo ordine |
| Deal Days between Create and KYC | Giorni tra creazione e KYC |
| Deal Date entered "Proposal sent" | Data invio proposal |
| Deal Date exited "Proposal sent" | Data uscita da proposal |
| Deal Cumulative time in "Proposal sent" (min) | Tempo cumulativo in proposal (minuti) |
| Ore in Proposal sent | Ore in Proposal (calcolato) |
| Risk Check Status | Stato risk check |
| Store Type | Tipo di store |

## Setup Locale

### 1. Clona il repository
```bash
git clone https://github.com/scalapaymktg/b2b-partner-tracker.git
cd b2b-partner-tracker
```

### 2. Crea ambiente virtuale
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# oppure: venv\Scripts\activate  # Windows
```

### 3. Installa dipendenze
```bash
pip install -r requirements.txt
```

### 4. Configura variabili d'ambiente
Crea un file `.env`:
```env
HUBSPOT_API_TOKEN=pat-eu1-xxxxx
GOOGLE_SHEET_ID=1JtvLP9vLPkn98seLav0tUQShvQyICSfLA87eP-cv7uk
```

### 5. Configura Google Sheets API
- Crea un progetto su [Google Cloud Console](https://console.cloud.google.com)
- Abilita Google Sheets API
- Crea credenziali OAuth 2.0
- Scarica `credentials.json` nella cartella del progetto
- Esegui lo script una volta per generare `token.json`

### 6. Esegui
```bash
# Esecuzione singola
python hubspot_to_sheets.py

# Esecuzione schedulata (ogni giorno alle 05:05)
python hubspot_to_sheets.py --schedule
```

## GitHub Actions

Il workflow esegue automaticamente l'export ogni giorno alle 05:05 CET.

### Repository Secrets Richiesti

| Secret | Descrizione |
|--------|-------------|
| `HUBSPOT_API_TOKEN` | Token API HubSpot |
| `GOOGLE_SHEET_ID` | ID del Google Sheet |
| `GOOGLE_CREDENTIALS` | Contenuto di `credentials.json` |
| `GOOGLE_TOKEN` | Contenuto di `token.json` |

### Esecuzione Manuale
1. Vai su Actions → "HubSpot to Google Sheets Export"
2. Clicca "Run workflow"

## Output

I dati vengono esportati su: [Google Sheet](https://docs.google.com/spreadsheets/d/1JtvLP9vLPkn98seLav0tUQShvQyICSfLA87eP-cv7uk)

## Struttura File

```
b2b-partner-tracking/
├── .github/
│   └── workflows/
│       └── export.yml          # GitHub Actions workflow
├── .gitignore                  # File ignorati da git
├── credentials.json            # Credenziali Google (non in git)
├── token.json                  # Token OAuth Google (non in git)
├── .env                        # Variabili d'ambiente (non in git)
├── hubspot_to_sheets.py        # Script principale
├── requirements.txt            # Dipendenze Python
└── README.md                   # Documentazione
```

## Tecnologie

- Python 3.9+
- HubSpot CRM API v3
- Google Sheets API v4
- GitHub Actions per scheduling

## Changelog

### v1.0.0
- Export deal da Partnership Pipeline
- Filtro per 4 partner (SmallPay, Deutsche Bank, Attitude, PostePay)
- Colonne partner-specifiche
- Colonna calcolata "Ore in Proposal sent"
- Formattazione Euro per Amount e TTV
- GitHub Actions per esecuzione giornaliera alle 05:05 CET
