#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:?app dir required}"
ARCHIVE="${2:?archive path required}"
APP_NAME="rpf-origin-server"

for profile in "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.bashrc"; do
  if [[ -f "$profile" ]]; then
    # shellcheck disable=SC1090
    source "$profile"
  fi
done

if [[ -d "$HOME/.nvm/versions/node" ]]; then
  latest_node_bin="$(find "$HOME/.nvm/versions/node" -path '*/bin/node' -printf '%h\n' 2>/dev/null | sort | tail -n 1 || true)"
  if [[ -n "$latest_node_bin" ]]; then
    export PATH="$latest_node_bin:$PATH"
  fi
fi

if ! command -v pm2 >/dev/null 2>&1; then
  if [[ -d "$HOME/.nvm/versions/node" ]]; then
    latest_pm2_dir="$(find "$HOME/.nvm/versions/node" -path '*/bin/pm2' -printf '%h\n' 2>/dev/null | sort | tail -n 1 || true)"
    if [[ -n "$latest_pm2_dir" ]]; then
      export PATH="$latest_pm2_dir:$PATH"
    fi
  fi
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$APP_DIR" "$APP_DIR/logs"
tar -xf "$ARCHIVE" -C "$TMP_DIR"

if [[ -f "$TMP_DIR/server" ]]; then
  install -m 0755 "$TMP_DIR/server" "$APP_DIR/server.new"
  rm -f "$TMP_DIR/server"
fi

cp -a "$TMP_DIR"/. "$APP_DIR"/
rm -f "$ARCHIVE"

if [[ -f "$APP_DIR/server.new" ]]; then
  mv -f "$APP_DIR/server.new" "$APP_DIR/server"
fi

chmod +x "$APP_DIR/server"

if [[ ! -f "$APP_DIR/config.toml" ]]; then
  cp "$APP_DIR/config.example.toml" "$APP_DIR/config.toml"
  echo "Created default config.toml from config.example.toml. Update Mongo Atlas URL before exposing the origin."
fi

set_web_config_value() {
  local key="${1:?config key required}"
  local value="${2:?config value required}"
  local config_file="$APP_DIR/config.toml"
  local updated_file="$TMP_DIR/config.toml.updated"

  awk -v key="$key" -v value="$value" '
    BEGIN {
      in_web = 0
      seen_web = 0
      done = 0
    }
    /^\[web\][[:space:]]*$/ {
      in_web = 1
      seen_web = 1
      print
      next
    }
    /^\[/ {
      if (in_web && !done) {
        print key " = " value
        done = 1
      }
      in_web = 0
      print
      next
    }
    in_web && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      print key " = " value
      done = 1
      next
    }
    {
      print
    }
    END {
      if (!done) {
        if (!seen_web) {
          print ""
          print "[web]"
        }
        print key " = " value
      }
    }
  ' "$config_file" > "$updated_file"

  mv "$updated_file" "$config_file"
}

set_web_config_value "ingest_rate_limit_contribute_per_minute" "1200"
set_web_config_value "ingest_rate_limit_multiple_per_minute" "600"
set_web_config_value "ingest_rate_limit_players_per_minute" "1200"
set_web_config_value "ingest_rate_limit_detail_per_minute" "1200"
set_web_config_value "ingest_rate_limit_fflogs_jobs_per_minute" "600"
set_web_config_value "ingest_rate_limit_fflogs_results_per_minute" "1200"

if ! command -v pm2 >/dev/null 2>&1; then
  echo "PATH=$PATH" >&2
  echo "pm2 is required on the Raspberry Pi" >&2
  exit 1
fi

pushd "$APP_DIR" >/dev/null
echo "Using pm2 from: $(command -v pm2)"
if pm2 describe "$APP_NAME" >/dev/null 2>&1; then
  pm2 reload "$APP_NAME" --update-env
else
  pm2 start ecosystem.origin-server.config.js
fi
pm2 save
popd >/dev/null

echo "Origin server deployed to $APP_DIR"
