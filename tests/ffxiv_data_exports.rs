use remote_party_finder_reborn::ffxiv::{self, duties::ContentKind};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn generated_ffxiv_data_remains_available_from_server_ffxiv_module() {
    let auto_translate = ffxiv::AUTO_TRANSLATE
        .get(&(1, 100))
        .expect("representative auto-translate entry should exist");
    assert_eq!(auto_translate.en, "【Languages】");
    assert_eq!(auto_translate.ja, "【言葉】");

    let duty = ffxiv::DUTIES
        .get(&1)
        .expect("representative duty entry should exist");
    assert_eq!(duty.name.en, "The Thousand Maws of Toto-Rak");
    assert!(matches!(duty.content_kind, ContentKind::Dungeons));

    let territory = ffxiv::TERRITORY_NAMES
        .get(&128)
        .expect("representative territory entry should exist");
    assert_eq!(territory.en, "Limsa Lominsa Upper Decks");
    assert_eq!(territory.fr, "Limsa Lominsa - Le Tillac");
}

#[test]
fn generated_ffxiv_data_is_sourced_from_path_crate() {
    let auto_translate = ffxiv_data::AUTO_TRANSLATE
        .get(&(1, 100))
        .expect("representative auto-translate entry should exist in path crate");
    assert_eq!(
        auto_translate.en,
        ffxiv::AUTO_TRANSLATE.get(&(1, 100)).unwrap().en
    );

    let duty = ffxiv_data::DUTIES
        .get(&1)
        .expect("representative duty entry should exist in path crate");
    assert_eq!(duty.name.en, ffxiv::DUTIES.get(&1).unwrap().name.en);

    let territory = ffxiv_data::TERRITORY_NAMES
        .get(&128)
        .expect("representative territory entry should exist in path crate");
    assert_eq!(territory.en, ffxiv::TERRITORY_NAMES.get(&128).unwrap().en);
}

#[test]
fn listing_data_generator_reads_extracted_ffxiv_data_crate() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(node) = node_binary() else {
        eprintln!("skipping listing-data generator smoke test: node is not available");
        return;
    };
    let output_path = manifest_dir
        .join("target")
        .join("test-output")
        .join("listing-data-smoke.js");
    let _ = fs::remove_file(&output_path);

    let default_output = Command::new(&node)
        .arg("scripts/generate-listing-data.js")
        .arg("--print-output-path")
        .current_dir(&manifest_dir)
        .output()
        .expect("node should print listing-data generator output path");
    assert!(
        default_output.status.success(),
        "listing-data generator should report its default output path"
    );
    let default_output =
        String::from_utf8(default_output.stdout).expect("default output path should be utf8");
    let expected_web_asset = manifest_dir
        .parent()
        .expect("server repo should have a parent directory")
        .join("RemotePartyFinderReborn-Web")
        .join("public")
        .join("assets")
        .join("listing-data.js");
    let expected_web_asset_dir = expected_web_asset
        .parent()
        .expect("expected Web asset should have a parent directory");
    if expected_web_asset_dir.exists() {
        assert_eq!(
            PathBuf::from(default_output.trim()),
            expected_web_asset,
            "default listing-data output should target the Web public asset"
        );
    } else {
        eprintln!(
            "skipping default Web output assertion: sibling Web asset not found at {}",
            expected_web_asset.display()
        );
    }

    let status = Command::new(&node)
        .arg("scripts/generate-listing-data.js")
        .arg(format!("--out={}", output_path.display()))
        .current_dir(&manifest_dir)
        .stdout(Stdio::null())
        .status()
        .expect("node should run listing data generator");

    assert!(status.success(), "listing-data generation should succeed");
    let generated = fs::read_to_string(&output_path)
        .expect("listing-data generator should write the requested output file");
    assert!(generated.contains("The Thousand Maws of Toto-Rak"));
    assert!(generated.contains("Limsa Lominsa Upper Decks"));
    assert!(generated.contains("【Languages】"));
}

#[test]
fn translations_generator_includes_web_runtime_status_keys() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(dotnet) = dotnet_binary() else {
        eprintln!("skipping translations generator smoke test: dotnet is not available");
        return;
    };

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "rpf-translations-generator-test-{}-{unique}",
        std::process::id()
    ));
    let csv_root = root.join("datamine");
    let out_dir = root.join("out");
    fs::create_dir_all(csv_root.join("csv")).expect("test CSV root should be created");
    fs::create_dir_all(&out_dir).expect("test output directory should be created");

    let status = Command::new(&dotnet)
        .arg("run")
        .arg("--project")
        .arg("generator/SourceGenerator.csproj")
        .arg("--")
        .arg(&csv_root)
        .arg(&out_dir)
        .current_dir(&manifest_dir)
        .stdout(Stdio::null())
        .status()
        .expect("dotnet should run the translations generator");

    assert!(status.success(), "translations generation should succeed");

    let generated = fs::read_to_string(out_dir.join("translations.js"))
        .expect("translations generator should write translations.js");
    let _ = fs::remove_dir_all(&root);
    assert!(
        generated.contains("service_unavailable"),
        "translations.js should include the 503 status translation used by the Web client"
    );
}

fn node_binary() -> Option<String> {
    ["node", "node.exe"].into_iter().find_map(|candidate| {
        Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .filter(|status| status.success())
            .map(|_| candidate.to_string())
    })
}

fn dotnet_binary() -> Option<String> {
    ["dotnet", "dotnet.exe"].into_iter().find_map(|candidate| {
        Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .filter(|status| status.success())
            .map(|_| candidate.to_string())
    })
}
