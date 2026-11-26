//! FFLogs 관련 유틸리티
//!
//! 서버 이름에서 FFLogs 리전을 추출하는 기능을 제공합니다.

use ffxiv_types::{DataCenter, World};

/// 서버 이름에서 리전 추출
pub fn get_region_from_server(server: &str) -> &'static str {
    let Ok(world) = server.trim().parse::<World>() else {
        return "NA"; // Default fallback when server name cannot be parsed
    };

    match world.data_center() {
        DataCenter::Elemental | DataCenter::Gaia | DataCenter::Mana | DataCenter::Meteor => "JP",
        DataCenter::Aether | DataCenter::Primal | DataCenter::Crystal | DataCenter::Dynamis => "NA",
        DataCenter::Chaos | DataCenter::Light | DataCenter::Shadow => "EU",
        DataCenter::Materia => "OC",
    }
}

#[cfg(test)]
mod tests {
    use super::get_region_from_server;
    use ffxiv_types::DataCenter;

    #[test]
    fn maps_shadow_dc_worlds_to_eu_and_covers_all_data_centers() {
        for shadow_world in ["Innocence", "Pixie", "Titania", "Tycoon", " titania "] {
            assert_eq!(get_region_from_server(shadow_world), "EU");
        }

        // Verify representative worlds across every DC + case insensitivity.
        assert_eq!(get_region_from_server("tonberry"), "JP");
        assert_eq!(get_region_from_server("Bahamut"), "JP");
        assert_eq!(get_region_from_server("Chocobo"), "JP");
        assert_eq!(get_region_from_server("Shinryu"), "JP");

        assert_eq!(get_region_from_server("Gilgamesh"), "NA");
        assert_eq!(get_region_from_server("Leviathan"), "NA");
        assert_eq!(get_region_from_server("Balmung"), "NA");
        assert_eq!(get_region_from_server("Seraph"), "NA");

        assert_eq!(get_region_from_server("Moogle"), "EU");
        assert_eq!(get_region_from_server("Odin"), "EU");

        assert_eq!(get_region_from_server("Ravana"), "OC");
        assert_eq!(get_region_from_server("Sephirot"), "OC");

        // Fallback to NA only when parse fails.
        assert_eq!(get_region_from_server(""), "NA");
        assert_eq!(get_region_from_server("UnknownWorld"), "NA");
    }

    #[test]
    fn worlds_catalog_includes_shadow_dc_and_maps_every_world_exhaustively() {
        for (world_id, expected_name) in [
            (412_u32, "Titania"),
            (413_u32, "Innocence"),
            (414_u32, "Pixie"),
            (415_u32, "Tycoon"),
        ] {
            let world = crate::ffxiv::WORLDS
                .get(&world_id)
                .unwrap_or_else(|| panic!("Shadow DC world {world_id} missing from WORLDS"));
            assert_eq!(world.name(), expected_name);
            assert_eq!(world.data_center(), DataCenter::Shadow);
            assert_eq!(get_region_from_server(world.as_str()), "EU");
        }

        for world in crate::ffxiv::WORLDS.values() {
            let expected = match world.data_center() {
                DataCenter::Elemental
                | DataCenter::Gaia
                | DataCenter::Mana
                | DataCenter::Meteor => "JP",
                DataCenter::Aether
                | DataCenter::Primal
                | DataCenter::Crystal
                | DataCenter::Dynamis => "NA",
                DataCenter::Chaos | DataCenter::Light | DataCenter::Shadow => "EU",
                DataCenter::Materia => "OC",
            };
            assert_eq!(get_region_from_server(world.as_str()), expected);
        }
    }
}
