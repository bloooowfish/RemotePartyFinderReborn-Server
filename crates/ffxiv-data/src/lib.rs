use std::{cmp::Ordering, str::FromStr};

use serde::Serialize;

pub use self::{
    auto_translate::AUTO_TRANSLATE, duties::DUTIES, territory_names::TERRITORY_NAMES,
};

pub mod auto_translate;
pub mod duties;
pub mod territory_names;

#[derive(Debug, Copy, Clone, Serialize)]
pub struct LocalisedText {
    pub en: &'static str,
    pub ja: &'static str,
    pub de: &'static str,
    pub fr: &'static str,
}

impl LocalisedText {
    pub fn text(&self, lang: &Language) -> &'static str {
        match lang {
            Language::English => self.en,
            Language::Japanese => self.ja,
            Language::German => self.de,
            Language::French => self.fr,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Language {
    English,
    Japanese,
    German,
    French,
}

impl Language {
    pub fn code(&self) -> &'static str {
        match self {
            Self::English => "en",
            Self::Japanese => "ja",
            Self::German => "de",
            Self::French => "fr",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::English => "english",
            Self::Japanese => "日本語",
            Self::German => "deutsch",
            Self::French => "français",
        }
    }

    pub fn from_codes(val: Option<&str>) -> Self {
        let val = match val {
            Some(v) => v,
            None => return Self::English,
        };

        let mut parts: Vec<(&str, f32, usize)> = val
            .split(',')
            .enumerate()
            .map(|(index, part)| {
                let language = part.split(';').next().unwrap_or("").trim();
                let quality = part
                    .split(';')
                    .skip(1)
                    .find_map(|param| {
                        param
                            .trim()
                            .strip_prefix("q=")
                            .map(|value| f32::from_str(value).unwrap_or(0.0))
                    })
                    .unwrap_or(1.0);

                (language, quality, index)
            })
            .collect();
        parts.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.2.cmp(&b.2))
        });
        if parts.is_empty() {
            return Self::English;
        }

        for (lang, _, _) in parts {
            let first = lang.split('-').next().unwrap();
            match first {
                "en" => return Self::English,
                "ja" => return Self::Japanese,
                "de" => return Self::German,
                "fr" => return Self::French,
                _ => {}
            }
        }

        Self::English
    }
}
