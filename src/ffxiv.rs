use crate::listing::{DutyCategory, DutyType};
use std::borrow::Cow;

pub use self::{
    duties::DUTIES, jobs::JOBS, roulettes::ROULETTES, territory_names::TERRITORY_NAMES,
    treasure_maps::TREASURE_MAPS, worlds::WORLDS,
};
pub use ffxiv_data::auto_translate::AUTO_TRANSLATE;
pub use ffxiv_data::Language;

pub mod auto_translate {
    pub use ffxiv_data::auto_translate::*;
}

pub mod duties {
    pub use ffxiv_data::duties::*;
}

pub mod jobs;
pub mod roulettes;
pub mod territory_names {
    pub use ffxiv_data::territory_names::*;
}

pub mod treasure_maps;
pub mod worlds;

#[cfg(test)]
mod language_tests {
    use super::Language;

    #[test]
    fn from_codes_prefers_higher_q_value() {
        assert!(matches!(
            Language::from_codes(Some("en-US;q=0.4,en;q=0.3,ja;q=0.9")),
            Language::Japanese
        ));
    }

    #[test]
    fn from_codes_keeps_header_order_when_q_values_match() {
        assert!(matches!(
            Language::from_codes(Some("fr-FR;q=0.8,de-DE;q=0.8,en-US;q=0.2")),
            Language::French
        ));
    }
}

pub use ffxiv_data::LocalisedText;

pub fn duty(duty: u32) -> Option<&'static duties::DutyInfo> {
    crate::ffxiv::DUTIES
        .get(&duty)
        .or_else(|| old::OLD_DUTIES.get(&duty))
}

pub fn roulette(roulette: u32) -> Option<&'static roulettes::RouletteInfo> {
    crate::ffxiv::ROULETTES
        .get(&roulette)
        .or_else(|| old::OLD_ROULETTES.get(&roulette))
}

pub fn duty_name<'a>(
    duty_type: DutyType,
    category: DutyCategory,
    duty: u16,
    lang: Language,
) -> Cow<'a, str> {
    match (duty_type, category) {
        (DutyType::Other, DutyCategory::Fate) => {
            if let Some(name) = crate::ffxiv::TERRITORY_NAMES.get(&u32::from(duty)) {
                return Cow::from(name.text(&lang));
            }

            return Cow::from("FATEs");
        }
        (DutyType::Other, DutyCategory::TheHunt) => {
            return Cow::from(match lang {
                Language::English => "The Hunt",
                Language::Japanese => "モブハント",
                Language::German => "Hohe Jagd",
                Language::French => "Contrats de chasse",
            })
        }
        (_, DutyCategory::None) if duty == 0 => {
            return Cow::from(match lang {
                Language::English => "None",
                Language::Japanese => "設定なし",
                Language::German => "Nicht festgelegt",
                Language::French => "Non spécifiée",
            })
        }
        (DutyType::Other, DutyCategory::DeepDungeon) if duty == 1 => {
            return Cow::from(match lang {
                Language::English => "The Palace of the Dead",
                Language::Japanese => "死者の宮殿",
                Language::German => "Palast der Toten",
                Language::French => "Palais des morts",
            })
        }
        (DutyType::Other, DutyCategory::DeepDungeon) if duty == 2 => {
            return Cow::from(match lang {
                Language::English => "Heaven-on-High",
                Language::Japanese => "アメノミハシラ",
                Language::German => "Himmelssäule",
                Language::French => "Pilier des Cieux",
            })
        }
        (DutyType::Other, DutyCategory::DeepDungeon) if duty == 3 => {
            return Cow::from(match lang {
                Language::English => "Eureka Orthos",
                Language::Japanese => "オルト・エウレカ",
                Language::German => "Eureka Orthos",
                Language::French => "Eurêka Orthos",
            })
        }
        (DutyType::Normal, _) => {
            if let Some(info) = crate::ffxiv::duty(u32::from(duty)) {
                return Cow::from(info.name.text(&lang));
            }
        }
        (DutyType::Roulette, _) => {
            if let Some(info) = roulette(u32::from(duty)) {
                return Cow::from(info.name.text(&lang));
            }
        }
        // gold saucer duties appear to be all over the place. source in comments:
        (_, DutyCategory::GoldSaucer) if duty == 11 => {
            return Cow::from(match lang {
                // Addon 2308
                Language::English => "GATEs",
                Language::Japanese => "G.A.T.E.",
                Language::German => "GATEs",
                Language::French => "JACTA",
            });
        }
        (_, DutyCategory::GoldSaucer) if (12..=19).contains(&duty) => {
            // in the sheet, the order is sagolii, del sol, tranquil, random
            // in PF, random comes first:
            let row = match duty {
                12 | 16 => 21 + (duty - 12),
                13..=15 => 18 + (duty - 13),
                17..=19 => 22 + (duty - 17),
                _ => 0,
            };
            if let Some(info) = roulette(u32::from(row)) {
                return Cow::from(info.name.text(&lang));
            }
        }
        (_, DutyCategory::GoldSaucer) if (20..=26).contains(&duty) => {
            let row = match duty {
                20 => 195,
                21 => 756,
                22 => 199,
                23 => 645,
                24 => 650,
                25 => 768,
                26 => 769,
                _ => 0,
            };

            if let Some(info) = crate::ffxiv::duty(row) {
                return Cow::from(info.name.text(&lang));
            }
        }
        (_, DutyCategory::TreasureHunt) => {
            if let Some(name) = crate::ffxiv::TREASURE_MAPS.get(&u32::from(duty)) {
                return Cow::from(name.text(&lang));
            }
        }
        _ => {}
    }

    tracing::debug!(
        "unknown type/category/duty: {:?}/{:?}/{}",
        duty_type,
        category,
        duty
    );
    Cow::from(format!("{:?}", category))
}

mod old {
    use std::collections::HashMap;

    use crate::ffxiv::{
        duties::{ContentKind, DutyInfo},
        roulettes::RouletteInfo,
        LocalisedText,
    };

    lazy_static::lazy_static! {
        pub static ref OLD_DUTIES: HashMap<u32, DutyInfo> = maplit::hashmap! {
            62 => DutyInfo {
                name: LocalisedText {
                    en: "Cape Westwind",
                    ja: "リットアティン強襲戦",
                    de: "Kap Westwind",
                    fr: "Le Cap Vendouest",
                },
                content_kind: ContentKind::Trials,
            },
            83 => DutyInfo {
                name: LocalisedText {
                    en: "The Steps of Faith",
                    ja: "皇都イシュガルド防衛戦",
                    de: "Der Schicksalsweg",
                    fr: "Le Siège de la sainte Cité d'Ishgard",
                },
                content_kind: ContentKind::Trials,
            },
            143 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (4 on 4 - Training)",
                    ja: "ザ・フィースト (4対4 / カジュアルマッチ)",
                    de: "The Feast (4 gegen 4, Übungskampf)",
                    fr: "The Feast (4x4/entraînement)",
                },
                content_kind: ContentKind::PvP,
            },
            145 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (4 on 4 - Ranked)",
                    ja: "ザ・フィースト (4対4 / ランクマッチ)",
                    de: "The Feast (4 gegen 4, gewertet)",
                    fr: "The Feast (4x4/classé)",
                },
                content_kind: ContentKind::PvP,
            },
            201 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Custom Match - Feasting Grounds)",
                    ja: "ザ・フィースト (ウルヴズジェイル演習場：カスタムマッチ）",
                    de: "The Feast (Wolfshöhle: Schaukampf)",
                    fr: "The Feast (personnalisé/Festin des loups)",
                },
                content_kind: ContentKind::PvP,
            },
            228 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (4 on 4 - Training)",
                    ja: "ザ・フィースト (4対4 / カジュアルマッチ)",
                    de: "The Feast (4 gegen 4, Übungskampf)",
                    fr: "The Feast (4x4/entraînement)",
                },
                content_kind: ContentKind::PvP,
            },
            230 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (4 on 4 - Ranked)",
                    ja: "ザ・フィースト (4対4 / ランクマッチ)",
                    de: "The Feast (4 gegen 4, gewertet)",
                    fr: "The Feast (4x4/classé)",
                },
                content_kind: ContentKind::PvP,
            },
            233 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Custom Match - Lichenweed)",
                    ja: "ザ・フィースト (ライケンウィード演習場：カスタムマッチ）",
                    de: "The Feast (Flechtenhain: Schaukampf)",
                    fr: "The Feast (personnalisé/Pré-de-lichen)",
                },
                content_kind: ContentKind::PvP,
            },
            476 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Team Ranked)",
                    ja: "ザ・フィースト (チーム用ランクマッチ)",
                    de: "The Feast (Team, gewertet)",
                    fr: "The Feast (classé/équipe JcJ)",
                },
                content_kind: ContentKind::PvP,
            },
            478 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Ranked)",
                    ja: "ザ・フィースト (ランクマッチ)",
                    de: "The Feast (gewertet)",
                    fr: "The Feast (classé)",
                },
                content_kind: ContentKind::PvP,
            },
            479 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Training)",
                    ja: "ザ・フィースト (カジュアルマッチ)",
                    de: "The Feast (Übungskampf)",
                    fr: "The Feast (entraînement)",
                },
                content_kind: ContentKind::PvP,
            },
            480 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Custom Match - Crystal Tower)",
                    ja: "ザ・フィースト (クリスタルタワー演習場：カスタムマッチ）",
                    de: "The Feast (Kristallturm-Arena: Schaukampf)",
                    fr: "The Feast (personnalisé/Tour de Cristal)",
                },
                content_kind: ContentKind::PvP,
            },
            580 => DutyInfo {
                name: LocalisedText {
                    en: "The Feast (Team Custom Match - Crystal Tower)",
                    ja: "ザ・フィースト (クリスタルタワー演習場：チーム用カスタムマッチ)",
                    de: "The Feast (Kristallturm-Arena: Team-Schaukampf) ",
                    fr: "The Feast (personnalisé/équipe JcJ/Tour de Cristal)",
                },
                content_kind: ContentKind::PvP,
            },
            776 => DutyInfo {
                name: LocalisedText {
                    en: "The Whorleater (Unreal)",
                    ja: "幻リヴァイアサン討滅戦",
                    de: "Traumprüfung - Leviathan",
                    fr: "Le Briseur de marées (irréel)",
                },
                content_kind: ContentKind::Trials,
            },
            821 => DutyInfo {
                name: LocalisedText {
                    en: "Ultima's Bane (Unreal)",
                    ja: "幻アルテマウェポン破壊作戦",
                    de: "Traumprüfung - Ultima",
                    fr: "Le fléau d'Ultima (irréel)",
                },
                content_kind: ContentKind::Trials,
            },
            875 => DutyInfo {
                name: LocalisedText {
                    en: "Containment Bay S1T7 (Unreal)",
                    ja: "幻魔神セフィロト討滅戦",
                    de: "Traumprüfung - Sephirot",
                    fr: "Unité de contention S1P7 (irréel)",
                },
                content_kind: ContentKind::Trials,
            },
        };

        pub static ref OLD_ROULETTES: HashMap<u32, RouletteInfo> = maplit::hashmap! {
            11 => RouletteInfo {
                name: LocalisedText {
                    en: "The Feast (Training Match)",
                    ja: "ザ・フィースト (カジュアルマッチ)",
                    de: "The Feast (Übungskampf)",
                    fr: "The Feast (entraînement)",
                },
                pvp: true,
            },
            13 => RouletteInfo {
                name: LocalisedText {
                    en: "The Feast (Ranked Match)",
                    ja: "ザ・フィースト (ランクマッチ)",
                    de: "The Feast (gewertet)",
                    fr: "The Feast (classé)",
                },
                pvp: true,
            },
            16 => RouletteInfo {
                name: LocalisedText {
                    en: "The Feast (Team Ranked Match)",
                    ja: "ザ・フィースト (チーム用ランクマッチ)",
                    de: "The Feast (Team, gewertet)",
                    fr: "The Feast (classé/équipe JcJ)",
                },
                pvp: true,
            },
        };
    }
}
