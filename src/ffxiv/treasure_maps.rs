use std::collections::HashMap;
use super::LocalisedText;

lazy_static::lazy_static! {
    pub static ref TREASURE_MAPS: HashMap<u32, LocalisedText> = maplit::hashmap! {
        0 => LocalisedText {
            en: "All Levels",
            ja: "レベルを指定しない",
            de: "Jede Stufe",
            fr: "Tous niveaux",
        },
        1 => LocalisedText {
            en: "Leather Treasure Map",
            ja: "古ぼけた地図G1",
            de: "Leder-Schatzkarte",
            fr: "Carte au trésor en cuir",
        },
        2 => LocalisedText {
            en: "Goatskin Treasure Map",
            ja: "古ぼけた地図G2",
            de: "Steinbockleder-Schatzkarte",
            fr: "Carte au trésor en peau de bouquetin",
        },
        3 => LocalisedText {
            en: "Toadskin Treasure Map",
            ja: "古ぼけた地図G3",
            de: "Krötenleder-Schatzkarte",
            fr: "Carte au trésor en peau de crapaud",
        },
        4 => LocalisedText {
            en: "Boarskin Treasure Map",
            ja: "古ぼけた地図G4",
            de: "Keilerleder-Schatzkarte",
            fr: "Carte au trésor en peau de sanglier",
        },
        5 => LocalisedText {
            en: "Peisteskin Treasure Map",
            ja: "古ぼけた地図G5",
            de: "Basiliskenleder-Schatzkarte",
            fr: "Carte au trésor en peau de peiste",
        },
        6 => LocalisedText {
            en: "Leather Buried Treasure Map",
            ja: "隠された地図G1",
            de: "Kryptische Karte",
            fr: "Carte au trésor secrète en cuir",
        },
        7 => LocalisedText {
            en: "Archaeoskin Treasure Map",
            ja: "古ぼけた地図G6",
            de: "Archaeoleder-Schatzkarte",
            fr: "Carte au trésor en peau d'archéornis",
        },
        8 => LocalisedText {
            en: "Wyvernskin Treasure Map",
            ja: "古ぼけた地図G7",
            de: "Wyvernleder-Schatzkarte",
            fr: "Carte au trésor en peau de wyverne",
        },
        9 => LocalisedText {
            en: "Dragonskin Treasure Map",
            ja: "古ぼけた地図G8",
            de: "Drachenleder-Schatzkarte",
            fr: "Carte au trésor en peau de dragon",
        },
        10 => LocalisedText {
            en: "Gaganaskin Treasure Map",
            ja: "古ぼけた地図G9",
            de: "Gaganaleder-Schatzkarte",
            fr: "Carte au trésor en peau de gagana",
        },
        11 => LocalisedText {
            en: "Gazelleskin Treasure Map",
            ja: "古ぼけた地図G10",
            de: "Gazellenleder-Schatzkarte",
            fr: "Carte au trésor en peau de gazelle",
        },
        12 => LocalisedText {
            en: "Seemingly Special Treasure Map",
            ja: "古ぼけた地図S1",
            de: "Exotenleder-Schatzkarte",
            fr: "Carte au trésor inhabituelle I",
        },
        13 => LocalisedText {
            en: "Gliderskin Treasure Map",
            ja: "古ぼけた地図G11",
            de: "Smilodonleder-Schatzkarte",
            fr: "Carte au trésor en peau de smilodon",
        },
        14 => LocalisedText {
            en: "Zonureskin Treasure Map",
            ja: "古ぼけた地図G12",
            de: "Glaucusleder-Schatzkarte",
            fr: "Carte au trésor en peau de glaucus",
        },
        15 => LocalisedText {
            en: "Ostensibly Special Treasure Map",
            ja: "古ぼけた地図S2",
            de: "Mythenleder-Schatzkarte",
            fr: "Carte au trésor inhabituelle II",
        },
        16 => LocalisedText {
            en: "Saigaskin Treasure Map",
            ja: "古ぼけた地図G13",
            de: "Gajaleder-Schatzkarte",
            fr: "Carte au trésor en peau de gaja",
        },
        17 => LocalisedText {
            en: "Kumbhiraskin Treasure Map",
            ja: "古ぼけた地図G14",
            de: "Kumbhilaleder-Schatzkarte",
            fr: "Carte au trésor en peau de kumbhira",
        },
        18 => LocalisedText {
            en: "Ophiotauroskin Treasure Map",
            ja: "古ぼけた地図G15",
            de: "Ophiotaurosleder-Schatzkarte",
            fr: "Carte au trésor en peau d'ophiotauros",
        },
        19 => LocalisedText {
            en: "Potentially Special Treasure Map",
            ja: "古ぼけた地図S3",
            de: "Legendenleder-Schatzkarte",
            fr: "Carte au trésor inhabituelle III",
        },
        20 => LocalisedText {
            en: "Conceivably Special Treasure Map",
            ja: "古ぼけた地図S4",
            de: "Sagenleder-Schatzkarte",
            fr: "Carte au trésor inhabituelle IV",
        },
        21 => LocalisedText {
            en: "Loboskin Treasure Map",
            ja: "古ぼけた地図G16",
            de: "Schakalleder-Schatzkarte",
            fr: "Carte au trésor en peau de loup argenté",
        },
        22 => LocalisedText {
            en: "Br'aaxskin Treasure Map",
            ja: "古ぼけた地図G17",
            de: "Br'aaxleder-Schatzkarte",
            fr: "Carte au trésor en peau de br'aax",
        },
        23 => LocalisedText {
            en: "Presumably Special Treasure Map",
            ja: "古ぼけた地図S5",
            de: "Epenleder-Schatzkarte",
            fr: "Carte au trésor inhabituelle V",
        },
        24 => LocalisedText {
            en: "Gargantuaskin Treasure Map",
            ja: "古ぼけた地図G18",
            de: "Gargantualeder-Schatzkarte",
            fr: "Carte au trésor en peau de gargantua",
        },
    };
}
