"""
Top 31 gesetzliche Krankenkassen in Deutschland (Account-Liste nach Mitgliederzahl).
"""

KASSEN = [
    {
        "name": "Techniker Krankenkasse",
        "short": "TK",
        "domain": "tk.de",
        "url": "https://www.tk.de",
        "linkedin_search": "Techniker Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/techniker-krankenkasse/",
    },
    {
        "name": "BARMER",
        "short": "BARMER",
        "domain": "barmer.de",
        "url": "https://www.barmer.de",
        "linkedin_search": "BARMER Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/barmer/",
    },
    {
        "name": "DAK-Gesundheit",
        "short": "DAK",
        "domain": "dak.de",
        "url": "https://www.dak.de",
        "linkedin_search": "DAK-Gesundheit",
        "linkedin_query_limit": 7,
        "linkedin_queries": [
            "DAK-Gesundheit",
            "DAK Vorstand",
            "Andreas Storm DAK",
            "Thomas Bodmer DAK",
            "Dr. Ute Wiedemann DAK",
            "DAK Digitalisierung",
            "DAK IT",
        ],
        "linkedin_url": "https://www.linkedin.com/company/dak-gesundheit/",
    },
    {
        "name": "IKK classic",
        "short": "IKK classic",
        "domain": "ikk-classic.de",
        "url": "https://www.ikk-classic.de",
        "linkedin_search": "IKK classic",
        "linkedin_url": "https://www.linkedin.com/company/ikk-classic/",
    },
    {
        "name": "Kaufmännische Krankenkasse (KKH)",
        "short": "KKH",
        "domain": "kkh.de",
        "url": "https://www.kkh.de",
        "linkedin_search": "KKH Kaufmännische Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/kkh-kaufmannische-krankenkasse/",
    },
    {
        "name": "Siemens-Betriebskrankenkasse (SBK)",
        "short": "SBK",
        "domain": "sbk.org",
        "url": "https://www.sbk.org",
        "linkedin_search": "SBK Siemens-Betriebskrankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/sbk-siemens-betriebskrankenkasse/",
    },
    {
        "name": "hkk (Handelskrankenkasse)",
        "short": "hkk",
        "domain": "hkk.de",
        "url": "https://www.hkk.de",
        "linkedin_search": "hkk Handelskrankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/hkk/",
    },
    {
        "name": "BKK firmus",
        "short": "BKK firmus",
        "domain": "bkk-firmus.de",
        "url": "https://www.bkk-firmus.de",
        "linkedin_search": "BKK firmus",
        "linkedin_url": "https://www.linkedin.com/company/bkk-firmus/",
    },
    {
        "name": "Mobil Krankenkasse",
        "short": "Mobil KK",
        "domain": "mobil-krankenkasse.de",
        "url": "https://www.mobil-krankenkasse.de",
        "linkedin_search": "Mobil Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/mobil-krankenkasse/",
    },
    {
        "name": "Audi BKK",
        "short": "Audi BKK",
        "domain": "audibkk.de",
        "url": "https://audibkk.de",
        "linkedin_search": "Audi BKK",
        "linkedin_url": "https://www.linkedin.com/company/audi-bkk/",
    },
    {
        "name": "VIACTIV Krankenkasse",
        "short": "VIACTIV",
        "domain": "viactiv.de",
        "url": "https://www.viactiv.de",
        "linkedin_search": "VIACTIV Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/viactiv-krankenkasse/",
    },
    {
        "name": "IKK Südwest",
        "short": "IKK Südwest",
        "domain": "ikk-suedwest.de",
        "url": "https://www.ikk-suedwest.de",
        "linkedin_search": "IKK Südwest",
        "linkedin_url": "https://www.linkedin.com/company/ikk-sudwest/",
    },
    {
        "name": "HEK – Hanseatische Krankenkasse",
        "short": "HEK",
        "domain": "hek.de",
        "url": "https://www.hek.de",
        "linkedin_search": "HEK Hanseatische Krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/hek-hanseatische-krankenkasse/",
    },
    {
        "name": "Pronova BKK",
        "short": "Pronova BKK",
        "domain": "pronovabkk.de",
        "url": "https://www.pronovabkk.de",
        "linkedin_search": "Pronova BKK",
        "linkedin_url": "https://www.linkedin.com/company/pronovabkk/",
    },
    {
        "name": "BAHN-BKK",
        "short": "BAHN-BKK",
        "domain": "bahn-bkk.de",
        "url": "https://www.bahn-bkk.de",
        "linkedin_search": "BAHN-BKK",
        "linkedin_url": "https://www.linkedin.com/company/bahn-bkk/",
    },
    {
        "name": "mkk – meine krankenkasse",
        "short": "mkk",
        "domain": "meine-krankenkasse.de",
        "url": "https://www.meine-krankenkasse.de",
        "linkedin_search": "mkk meine krankenkasse",
        "linkedin_url": "https://www.linkedin.com/company/mkk-meine-krankenkasse/",
    },
    {
        "name": "BIG direkt gesund",
        "short": "BIG direkt",
        "domain": "big-direkt.de",
        "url": "https://www.big-direkt.de",
        "linkedin_search": "BIG direkt gesund",
        "linkedin_url": "https://www.linkedin.com/company/big-direkt-gesund/",
    },
    {
        "name": "mhplus BKK",
        "short": "mhplus",
        "domain": "mhplus-krankenkasse.de",
        "url": "https://www.mhplus-krankenkasse.de",
        "linkedin_search": "mhplus BKK",
        "linkedin_url": "https://www.linkedin.com/company/mhplus-bkk/",
    },
    {
        "name": "IKK gesund plus",
        "short": "IKK gesund plus",
        "domain": "ikk-gesundplus.de",
        "url": "https://www.ikk-gesundplus.de",
        "linkedin_search": "IKK gesund plus",
        "linkedin_url": "https://www.linkedin.com/company/ikk-gesund-plus/",
    },
    {
        "name": "Novitas BKK",
        "short": "Novitas BKK",
        "domain": "novitas-bkk.de",
        "url": "https://www.novitas-bkk.de",
        "linkedin_search": "Novitas BKK",
        "linkedin_url": "https://www.linkedin.com/company/novitas-bkk/",
    },
    {
        "name": "vivida BKK",
        "short": "vivida BKK",
        "domain": "vividabkk.de",
        "url": "https://www.vividabkk.de",
        "linkedin_search": "vivida BKK",
        "linkedin_url": "https://www.linkedin.com/company/vivida-bkk/",
    },
    {
        "name": "BKK Linde",
        "short": "BKK Linde",
        "domain": "bkk-linde.de",
        "url": "https://www.bkk-linde.de",
        "linkedin_search": "BKK Linde",
        "linkedin_url": "https://www.linkedin.com/company/bkk-linde/",
    },
    {
        "name": "IK – Die Innovationskasse",
        "short": "IK Innovationskasse",
        "domain": "die-ik.de",
        "url": "https://www.die-ik.de",
        "linkedin_search": "Die Innovationskasse IK",
        "linkedin_url": "https://www.linkedin.com/company/die-innovationskasse/",
    },
    {
        "name": "Bosch BKK",
        "short": "Bosch BKK",
        "domain": "bosch-bkk.de",
        "url": "https://www.bosch-bkk.de",
        "linkedin_search": "Bosch BKK",
        "linkedin_url": "https://www.linkedin.com/company/bosch-bkk/",
    },
    {
        "name": "IKK Brandenburg und Berlin",
        "short": "IKK BB",
        "domain": "ikkbb.de",
        "url": "https://www.ikkbb.de",
        "linkedin_search": "IKK Brandenburg und Berlin",
        "linkedin_url": "https://www.linkedin.com/company/ikk-brandenburg-und-berlin/",
    },
    {
        "name": "SECURVITA BKK",
        "short": "SECURVITA",
        "domain": "securvita.de",
        "url": "https://www.securvita.de",
        "linkedin_search": "SECURVITA BKK",
        "linkedin_url": "https://www.linkedin.com/company/securvita-bkk/",
    },
    {
        "name": "Debeka BKK",
        "short": "Debeka BKK",
        "domain": "debeka-bkk.de",
        "url": "https://www.debeka-bkk.de",
        "linkedin_search": "Debeka BKK",
        "linkedin_url": "https://www.linkedin.com/company/debeka-bkk/",
    },
    {
        "name": "Salus BKK",
        "short": "Salus BKK",
        "domain": "salus-bkk.de",
        "url": "https://www.salus-bkk.de",
        "linkedin_search": "Salus BKK",
        "linkedin_url": "https://www.linkedin.com/company/salus-bkk/",
    },
    {
        "name": "R+V BKK",
        "short": "R+V BKK",
        "domain": "ruv-bkk.de",
        "url": "https://www.ruv-bkk.de",
        "linkedin_search": "R+V BKK",
        "linkedin_url": "https://www.linkedin.com/company/ruv-bkk/",
    },
    {
        "name": "BKK Gildemeister Seidensticker",
        "short": "BKK GS",
        "domain": "bkkgs.de",
        "url": "https://www.bkkgs.de",
        "linkedin_search": "BKK Gildemeister Seidensticker",
        "linkedin_url": "https://www.linkedin.com/company/bkk-gildemeister-seidensticker/",
    },
    {
        "name": "BKK Pfalz",
        "short": "BKK Pfalz",
        "domain": "bkk-pfalz.de",
        "url": "https://www.bkk-pfalz.de",
        "linkedin_search": "BKK Pfalz",
        "linkedin_url": "https://www.linkedin.com/company/bkk-pfalz/",
    },
]

# GKV-nahe Institutionen und Verbaende: Regulierung, TI/ePA/gematik,
# Datenschutz, Informationssicherheit und politische Marktbewegungen.
BEOBACHTETE_INSTITUTIONEN = [
    {
        "name": "Bundesministerium fuer Gesundheit",
        "short": "BMG",
        "type": "institution",
        "domain": "bundesgesundheitsministerium.de",
        "url": "https://www.bundesgesundheitsministerium.de",
        "linkedin_search": "Bundesministerium fuer Gesundheit",
        "linkedin_queries": ["BMG Digitalisierung Gesundheit", "Bundesministerium fuer Gesundheit GKV"],
        "news_queries": [
            'site:bundesgesundheitsministerium.de GKV Digitalisierung Gesetz',
            'site:bundesgesundheitsministerium.de ePA TI gematik Krankenkassen',
        ],
        "linkedin_url": "https://www.linkedin.com/company/bundesministerium-f-r-gesundheit/",
    },
    {
        "name": "GKV-Spitzenverband",
        "short": "GKV-SV",
        "type": "institution",
        "domain": "gkv-spitzenverband.de",
        "url": "https://www.gkv-spitzenverband.de",
        "linkedin_search": "GKV-Spitzenverband",
        "linkedin_queries": ["GKV-Spitzenverband Digitalisierung", "GKV-Spitzenverband ePA TI"],
        "news_queries": [
            'site:gkv-spitzenverband.de Digitalisierung GKV',
            'site:gkv-spitzenverband.de Stellungnahme ePA TI',
        ],
        "linkedin_url": "https://www.linkedin.com/company/gkv-spitzenverband/",
    },
    {
        "name": "gematik GmbH",
        "short": "gematik",
        "type": "institution",
        "domain": "gematik.de",
        "url": "https://www.gematik.de",
        "linkedin_search": "gematik",
        "linkedin_queries": ["gematik ePA TI", "gematik Digitalisierung Gesundheit"],
        "news_queries": [
            'site:gematik.de ePA TI Krankenkassen',
            'site:gematik.de Digitalisierung Gesundheit Interoperabilitaet',
        ],
        "linkedin_url": "https://www.linkedin.com/company/gematik/",
    },
    {
        "name": "Bundesamt fuer Sicherheit in der Informationstechnik",
        "short": "BSI",
        "type": "institution",
        "domain": "bsi.bund.de",
        "url": "https://www.bsi.bund.de",
        "linkedin_search": "Bundesamt fuer Sicherheit in der Informationstechnik",
        "linkedin_queries": ["BSI KRITIS NIS2 Gesundheit", "BSI Krankenkassen Informationssicherheit"],
        "news_queries": [
            'site:bsi.bund.de NIS2 KRITIS Gesundheit',
            'site:bsi.bund.de Krankenkassen Informationssicherheit',
        ],
        "linkedin_url": "https://www.linkedin.com/company/bundesamt-f-r-sicherheit-in-der-informationstechnik/",
    },
    {
        "name": "AOK-Bundesverband",
        "short": "AOK-BV",
        "type": "institution",
        "domain": "aok-bv.de",
        "url": "https://www.aok-bv.de",
        "linkedin_search": "AOK-Bundesverband",
        "linkedin_queries": ["AOK-Bundesverband Digitalisierung", "AOK-Bundesverband GKV IT"],
        "news_queries": ['site:aok-bv.de Digitalisierung GKV', 'site:aok-bv.de Stellungnahme Gesetz'],
        "linkedin_url": "https://www.linkedin.com/company/aok-bundesverband/",
    },
    {
        "name": "vdek Verband der Ersatzkassen",
        "short": "vdek",
        "type": "institution",
        "domain": "vdek.com",
        "url": "https://www.vdek.com",
        "linkedin_search": "Verband der Ersatzkassen vdek",
        "linkedin_queries": ["vdek Digitalisierung GKV", "vdek ePA TI"],
        "news_queries": ['site:vdek.com Digitalisierung GKV', 'site:vdek.com ePA TI Stellungnahme'],
        "linkedin_url": "https://www.linkedin.com/company/verband-der-ersatzkassen-e-v-vdek/",
    },
    {
        "name": "IKK e.V.",
        "short": "IKK e.V.",
        "type": "institution",
        "domain": "ikkev.de",
        "url": "https://www.ikkev.de",
        "linkedin_search": "IKK e.V.",
        "linkedin_queries": ["IKK e.V. Digitalisierung", "IKK e.V. GKV"],
        "news_queries": ['site:ikkev.de Digitalisierung GKV', 'site:ikkev.de Stellungnahme Gesetz'],
        "linkedin_url": "https://www.linkedin.com/company/ikk-e-v/",
    },
    {
        "name": "BKK Dachverband",
        "short": "BKK DV",
        "type": "institution",
        "domain": "bkk-dachverband.de",
        "url": "https://www.bkk-dachverband.de",
        "linkedin_search": "BKK Dachverband",
        "linkedin_queries": ["BKK Dachverband Digitalisierung", "BKK Dachverband GKV"],
        "news_queries": ['site:bkk-dachverband.de Digitalisierung GKV', 'site:bkk-dachverband.de Stellungnahme Gesetz'],
        "linkedin_url": "https://www.linkedin.com/company/bkk-dachverband-e-v/",
    },
]

# GKV-IT-Dienstleister: werden nur im LinkedIn-Radar beobachtet,
# nicht im Web-Research-Batch oder TED-Abruf.
BEOBACHTETE_ORGS = [
    {
        "name": "BITMARCK Unternehmensgruppe",
        "short": "BITMARCK",
        "type": "provider",
        "domain": "bitmarck.de",
        "url": "https://www.bitmarck.de",
        "linkedin_search": "BITMARCK",
        "linkedin_query_limit": 8,
        "linkedin_queries": [
            "BITMARCK Kundentag",
            "BITMARCK-Kundentag",
            "BITMARCK Partnertag",
            "BITMARCK-Partnertag",
            "BITMARCK House of Health",
            "BITMARCK GKV",
            "BITMARCK Krankenkasse",
            "BITMARCK Daten Cloud KI",
            "BITMARCK Plattform",
            "BITMARCK",
        ],
        "news_queries": [
            '"BITMARCK Kundentag"',
            '"BITMARCK-Kundentag"',
            '"BITMARCK Partnertag"',
            '"BITMARCK-Partnertag"',
            '"House of Health" BITMARCK',
            'site:bitmarck.de Kundentag BITMARCK',
            'site:bitmarck.de Partnertag BITMARCK',
        ],
        "linkedin_url": "https://www.linkedin.com/company/bitmarck/",
    },
    {
        "name": "ITSC GmbH",
        "short": "ITSC",
        "type": "provider",
        "domain": "itsc.de",
        "url": "https://www.itsc.de",
        "linkedin_search": "ITSC GmbH",
        "linkedin_query_limit": 6,
        "linkedin_queries": [
            "ITSC Zukunftskongress",
            "ITSC-Zukunftskongress",
            "ITSC GmbH",
            "ITSC GKV",
            "ITSC Krankenkasse",
            "ITSC Plattform",
        ],
        "news_queries": [
            '"ITSC Zukunftskongress"',
            '"ITSC-Zukunftskongress"',
            'site:itsc.de Zukunftskongress ITSC',
        ],
        "linkedin_url": "https://www.linkedin.com/company/itsc-gmbh/",
    },
    {
        "name": "AOK Systems GmbH",
        "short": "AOK Systems",
        "type": "provider",
        "domain": "aok-systems.de",
        "url": "https://www.aok-systems.de",
        "linkedin_search": "AOK Systems GmbH",
        "linkedin_queries": ["AOK Systems", "AOK Systems GKV", "AOK Systems Krankenkasse"],
        "news_queries": [
            '"AOK Systems" GKV',
            '"AOK Systems" Krankenkasse',
            'site:aok-systems.de GKV OR Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/aok-systems-gmbh/",
    },
    {
        "name": "gkv informatik",
        "short": "gkv informatik",
        "type": "provider",
        "domain": "gkvi.de",
        "url": "https://www.gkvi.de",
        "linkedin_search": "gkv informatik",
        "linkedin_queries": ["gkv informatik", "gkvi Krankenkasse"],
        "news_queries": [
            '"gkv informatik" Krankenkasse',
            '"gkvi" Krankenkasse',
            'site:gkvi.de GKV OR Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/gkv-informatik/",
    },
    {
        "name": "gevko GmbH",
        "short": "gevko",
        "type": "provider",
        "domain": "gevko.de",
        "url": "https://www.gevko.de",
        "linkedin_search": "gevko GmbH",
        "linkedin_queries": ["gevko", "gevko GKV", "gevko Krankenkasse"],
        "linkedin_url": "https://www.linkedin.com/company/gevko-gmbh/",
    },
    {
        "name": "DAVASO GmbH",
        "short": "DAVASO",
        "type": "provider",
        "domain": "davaso.de",
        "url": "https://www.davaso.de",
        "linkedin_search": "DAVASO GmbH",
        "linkedin_queries": ["DAVASO", "DAVASO GKV", "DAVASO Krankenkasse"],
        "news_queries": [
            '"DAVASO" GKV',
            '"DAVASO" Krankenkasse',
            'site:davaso.de GKV OR Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/davaso-gmbh/",
    },
    {
        "name": "spectrumK GmbH",
        "short": "spectrumK",
        "type": "provider",
        "domain": "spectrumk.de",
        "url": "https://www.spectrumk.de",
        "linkedin_search": "spectrumK GmbH",
        "linkedin_queries": ["spectrumK", "spectrumK GKV", "spectrumK Krankenkasse"],
        "linkedin_url": "https://www.linkedin.com/company/spectrumk-gmbh/",
    },
    {
        "name": "adesso SE",
        "short": "adesso",
        "type": "provider",
        "domain": "adesso.de",
        "url": "https://www.adesso.de",
        "linkedin_search": "adesso SE",
        "linkedin_queries": ["adesso GKV", "adesso Krankenkasse", "adesso Health Krankenkasse"],
        "news_queries": [
            '"adesso" GKV',
            '"adesso" Krankenkasse',
            '"adesso" "gesetzliche Krankenversicherung"',
            'site:adesso.de GKV Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/adesso-se/",
    },
    {
        "name": "msg systems AG",
        "short": "msg",
        "type": "provider",
        "domain": "msg.group",
        "url": "https://www.msg.group",
        "linkedin_search": "msg systems ag",
        "linkedin_query_limit": 5,
        "linkedin_queries": ["msg GKV", "msg Krankenkasse", "msg Health Krankenkasse", "msg gesetzliche Krankenversicherung", "msg healthcare GKV"],
        "news_queries": [
            '"msg" GKV Krankenkasse',
            '"msg systems" Krankenkasse',
            '"msg" "gesetzliche Krankenversicherung"',
            'site:msg.group GKV Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/msg-systems-ag/",
    },
    {
        "name": "Materna Information & Communications SE",
        "short": "Materna",
        "type": "provider",
        "domain": "materna.de",
        "url": "https://www.materna.de",
        "linkedin_search": "Materna",
        "linkedin_queries": ["Materna GKV", "Materna Krankenkasse", "Materna Health Krankenkasse"],
        "news_queries": [
            '"Materna" GKV Krankenkasse',
            '"Materna" "gesetzliche Krankenversicherung"',
            'site:materna.de GKV Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/materna-information-&-communications-se/",
    },
    {
        "name": "Arvato Systems",
        "short": "Arvato Systems",
        "type": "provider",
        "domain": "arvato-systems.de",
        "url": "https://www.arvato-systems.de",
        "linkedin_search": "Arvato Systems",
        "linkedin_query_limit": 5,
        "linkedin_queries": ["Arvato Systems GKV", "Arvato Systems Krankenkasse", "Arvato Health Krankenkasse", "Arvato gesetzliche Krankenversicherung", "Arvato Cloud GKV"],
        "news_queries": [
            '"Arvato Systems" GKV',
            '"Arvato Systems" Krankenkasse',
            '"Arvato Systems" "gesetzliche Krankenversicherung"',
            'site:arvato-systems.de GKV Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/arvato-systems/",
    },
    {
        "name": "Sopra Steria",
        "short": "Sopra Steria",
        "type": "provider",
        "domain": "soprasteria.de",
        "url": "https://www.soprasteria.de",
        "linkedin_search": "Sopra Steria",
        "linkedin_queries": ["Sopra Steria GKV", "Sopra Steria Krankenkasse", "Sopra Steria Health Krankenkasse"],
        "news_queries": [
            '"Sopra Steria" GKV Krankenkasse',
            '"Sopra Steria" "gesetzliche Krankenversicherung"',
            'site:soprasteria.de GKV Krankenkasse',
        ],
        "linkedin_url": "https://www.linkedin.com/company/soprasteria/",
    },
]

# Branchenstimmen: Personen, deren LinkedIn-/News-Signale fuer einen grossen
# GKV-, Health-IT- und KI-Newsletter relevant sind, auch wenn ein Post nicht
# jedes Mal eine konkrete Krankenkasse nennt.
BEOBACHTETE_PERSONEN = [
    {
        "name": "Prof. Dr. David Matusiewicz",
        "short": "David Matusiewicz",
        "type": "influencer",
        "domain": "matusiewicz.de",
        "url": "https://www.david-matusiewicz.com",
        "linkedin_search": "Prof. Dr. David Matusiewicz",
        "linkedin_queries": [
            '"David Matusiewicz" Health IT',
            '"Prof. Dr. David Matusiewicz" KI Gesundheit',
            '"David Matusiewicz" Digitalisierung Gesundheitswesen',
            '"David Matusiewicz" Krankenkasse GKV',
        ],
        "news_queries": [
            '"David Matusiewicz" "KI" Gesundheit',
            '"David Matusiewicz" Digitalisierung Gesundheitswesen',
            '"David Matusiewicz" Health IT',
        ],
        "linkedin_url": "https://www.linkedin.com/in/david-matusiewicz/",
    },
    {
        "name": "Stefan Schellberg",
        "short": "Stefan Schellberg",
        "type": "top_voice",
        "domain": "ikk-classic.de",
        "url": "https://www.ikk-classic.de",
        "linkedin_search": "Stefan Schellberg IKK classic CDO",
        "linkedin_queries": [
            '"Stefan Schellberg" "IKK classic"',
            '"Stefan Schellberg" CDO Krankenkasse',
            '"Stefan Schellberg" Digitalisierung GKV',
        ],
        "news_queries": [
            '"Stefan Schellberg" "IKK classic" Digitalisierung',
            '"Stefan Schellberg" GKV IT',
        ],
    },
    {
        "name": "Andreas Strausfeld",
        "short": "Andreas Strausfeld",
        "type": "top_voice",
        "domain": "bitmarck.de",
        "url": "https://www.bitmarck.de",
        "linkedin_search": "Andreas Strausfeld BITMARCK CEO",
        "linkedin_queries": [
            '"Andreas Strausfeld" BITMARCK',
            '"Andreas Strausfeld" "Kundentag"',
            '"Andreas Strausfeld" GKV IT',
            '"Andreas Strausfeld" Krankenkasse Digitalisierung',
        ],
        "news_queries": [
            '"Andreas Strausfeld" BITMARCK',
            '"Andreas Strausfeld" "Kundentag"',
            '"Andreas Strausfeld" GKV IT',
        ],
    },
    {
        "name": "Dieter Loewe",
        "short": "Dieter Loewe",
        "type": "top_voice",
        "domain": "itsc.de",
        "url": "https://www.itsc.de",
        "linkedin_search": "Dieter Loewe ITSC Hannover CEO",
        "linkedin_queries": [
            '"Dieter Loewe" ITSC',
            '"Dieter Löwe" ITSC',
            '"Dieter Loewe" GKV IT',
            '"Dieter Löwe" GKV IT',
        ],
        "news_queries": [
            '"Dieter Loewe" ITSC Hannover',
            '"Dieter Löwe" ITSC Hannover',
            '"Dieter Loewe" GKV IT',
        ],
    },
    {
        "name": "DAK-Gesundheit Pressestelle",
        "short": "DAK Pressestelle",
        "type": "top_voice",
        "domain": "dak.de",
        "url": "https://www.dak.de",
        "linkedin_search": "DAK-Gesundheit Pressestelle Kommunikation",
        "linkedin_queries": [
            '"DAK-Gesundheit" Pressestelle',
            '"DAK-Gesundheit" Unternehmenskommunikation',
            '"DAK" Pressesprecher Digitalisierung Gesundheit',
            '"DAK" Kommunikation GKV Politik',
        ],
        "news_queries": [
            'site:dak.de Presse Digitalisierung DAK',
            'site:dak.de Presse GKV Politik DAK',
        ],
        "linkedin_url": "https://www.linkedin.com/company/dak-gesundheit/",
    },
]

from pathlib import Path
import re

TARGET_LIST_PRIMARY = Path('/mnt/data/kassen und linked In Liste.md')
TARGET_LIST_REPO = Path(__file__).resolve().parent / 'data' / 'kassen-und-linkedin-liste.md'

def _parse_target_list(md_text: str) -> list[dict]:
    entries, cur = [], {}
    for raw in md_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(('-', '*')):
            line = line[1:].strip()

        m_name = re.search(r'^(?:Kasse|Name|Organisation)\s*:\s*(.+)$', line, re.I)
        m_short = re.search(r'^(?:Short|Kurzname)\s*:\s*(.+)$', line, re.I)
        m_domain = re.search(r'^(?:Domain|Webseite|Website)\s*:\s*(https?://[^\s]+|[^\s]+)$', line, re.I)
        m_linkedin_url = re.search(r'(https?://(?:www\.)?linkedin\.com/[^\s\)]+)', line, re.I)
        m_linkedin_search = re.search(r'^(?:LinkedIn Suche|LinkedIn-Suche|LinkedIn Search)\s*:\s*(.+)$', line, re.I)

        if m_name:
            if cur.get('name'):
                entries.append(cur)
                cur = {}
            cur['name'] = m_name.group(1).strip()
            cur.setdefault('short', cur['name'][:24])
            continue
        if m_short:
            cur['short'] = m_short.group(1).strip()
            continue
        if m_domain:
            d = m_domain.group(1).strip().replace('https://', '').replace('http://', '').strip('/').split('/')[0]
            cur['domain'] = d
            cur.setdefault('url', f'https://{d}')
            continue
        if m_linkedin_search:
            cur['linkedin_search'] = m_linkedin_search.group(1).strip()
            continue
        if m_linkedin_url:
            cur['linkedin_url'] = m_linkedin_url.group(1).strip()
            cur.setdefault('linkedin_search', cur.get('name', ''))
            continue

    if cur.get('name'):
        entries.append(cur)

    out = []
    for e in entries:
        if not e.get('name'):
            continue
        e.setdefault('short', e['name'][:24])
        e.setdefault('domain', '')
        if not e.get('url') and e['domain']:
            e['url'] = f"https://{e['domain']}"
        e.setdefault('linkedin_search', e['name'])
        e.setdefault('linkedin_url', '')
        out.append(e)
    return out

def load_target_kassen() -> list[dict]:
    for path in (TARGET_LIST_PRIMARY, TARGET_LIST_REPO):
        try:
            if path.exists():
                parsed = _parse_target_list(path.read_text(encoding='utf-8'))
                if len(parsed) >= 20:
                    return parsed
        except Exception:
            pass
    return KASSEN
