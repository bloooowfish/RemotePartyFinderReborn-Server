param(
  [string]$BaseUrl = "http://127.0.0.1:18080",
  [int]$DurationSeconds = 600,
  [int]$IntervalSeconds = 5,
  [int]$LimitPerPoll = 200,
  [string]$OutputRoot = "",
  [string]$ComposeFile = "",
  [switch]$SkipMongo
)

$ErrorActionPreference = "Stop"

$ServerRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RepoRoot = (Resolve-Path (Join-Path $ServerRoot "..")).Path

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
  $OutputRoot = Join-Path $ServerRoot "output\listings-live-samples"
}

if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
  $ComposeFile = Join-Path $RepoRoot "docker-compose.live.yml"
}

$SnapshotDir = Join-Path $OutputRoot "snapshots"
$ListingDir = Join-Path $OutputRoot "listings"
$MongoListingDir = Join-Path $OutputRoot "mongo-listings"
$ManifestPath = Join-Path $OutputRoot "manifest.jsonl"

foreach ($path in @($OutputRoot, $SnapshotDir, $ListingDir, $MongoListingDir)) {
  New-Item -ItemType Directory -Force -Path $path | Out-Null
}

function Get-Sha256Hex {
  param([Parameter(Mandatory = $true)][string]$Text)

  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    return (($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join "")
  }
  finally {
    $sha.Dispose()
  }
}

function ConvertTo-CompactJson {
  param([Parameter(Mandatory = $true)]$Value)
  return ($Value | ConvertTo-Json -Depth 100 -Compress)
}

function ConvertTo-PrettyJson {
  param([Parameter(Mandatory = $true)]$Value)
  return ($Value | ConvertTo-Json -Depth 100)
}

function Add-ManifestEntry {
  param([Parameter(Mandatory = $true)]$Entry)
  Add-Content -Path $ManifestPath -Encoding UTF8 -Value ($Entry | ConvertTo-Json -Depth 20 -Compress)
}

function Save-RawSample {
  param(
    [Parameter(Mandatory = $true)][string]$Kind,
    [Parameter(Mandatory = $true)][string]$RawJson,
    [Parameter(Mandatory = $true)][string]$Directory,
    [Parameter(Mandatory = $true)]$Metadata
  )

  $hash = Get-Sha256Hex $RawJson
  $path = Join-Path $Directory "$Kind-$hash.json"
  if (Test-Path -LiteralPath $path) {
    return $false
  }

  Set-Content -Path $path -Encoding UTF8 -Value $RawJson

  $entry = [ordered]@{
    captured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    kind = $Kind
    sha256 = $hash
    path = $path
  }
  foreach ($key in $Metadata.Keys) {
    $entry[$key] = $Metadata[$key]
  }
  Add-ManifestEntry $entry
  return $true
}

function Save-JsonSample {
  param(
    [Parameter(Mandatory = $true)][string]$Kind,
    [Parameter(Mandatory = $true)]$Value,
    [Parameter(Mandatory = $true)][string]$Directory,
    [Parameter(Mandatory = $true)]$Metadata
  )

  $compactJson = ConvertTo-CompactJson $Value
  $hash = Get-Sha256Hex $compactJson
  $path = Join-Path $Directory "$Kind-$hash.json"
  if (Test-Path -LiteralPath $path) {
    return $false
  }

  Set-Content -Path $path -Encoding UTF8 -Value (ConvertTo-PrettyJson $Value)

  $entry = [ordered]@{
    captured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    kind = $Kind
    sha256 = $hash
    path = $path
  }
  foreach ($key in $Metadata.Keys) {
    $entry[$key] = $Metadata[$key]
  }
  Add-ManifestEntry $entry
  return $true
}

function Get-TextPayload {
  param($Payloads)

  if ($null -eq $Payloads) {
    return ""
  }

  $parts = New-Object System.Collections.Generic.List[string]
  foreach ($payload in @($Payloads)) {
    if ($null -ne $payload.text) {
      $parts.Add([string]$payload.text)
    }
  }
  return ($parts -join "")
}

function Get-ListingMetadata {
  param(
    [Parameter(Mandatory = $true)]$Container,
    [Parameter(Mandatory = $true)]$Revision
  )

  $listing = $Container.listing
  $metadata = [ordered]@{
    source = "api-snapshot"
    revision = $Revision
  }

  if ($null -ne $listing) {
    $metadata["duty_id"] = $listing.duty_id
    $metadata["creator_name"] = Get-TextPayload $listing.creator_name
    if ($null -ne $listing.home_world) {
      $metadata["home_world"] = $listing.home_world.name
    }
    if ($null -ne $listing.members) {
      $metadata["member_count"] = @($listing.members).Count
    }
  }

  return $metadata
}

function Save-ApiSnapshotSamples {
  $url = "$($BaseUrl.TrimEnd('/'))/api/listings/snapshot"
  $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 15
  $raw = [string]$response.Content
  $snapshot = $raw | ConvertFrom-Json
  $listings = @()
  if ($null -ne $snapshot.listings) {
    $listings = @($snapshot.listings)
  }

  $newSnapshots = 0
  $newListings = 0

  if ($listings.Count -gt 0) {
    $saved = Save-RawSample `
      -Kind "snapshot" `
      -RawJson $raw `
      -Directory $SnapshotDir `
      -Metadata ([ordered]@{
        source = "api-snapshot"
        revision = $snapshot.revision
        listing_count = $listings.Count
        url = $url
      })
    if ($saved) {
      $newSnapshots++
    }
  }

  foreach ($container in $listings) {
    $saved = Save-JsonSample `
      -Kind "listing" `
      -Value $container `
      -Directory $ListingDir `
      -Metadata (Get-ListingMetadata -Container $container -Revision $snapshot.revision)
    if ($saved) {
      $newListings++
    }
  }

  return [pscustomobject]@{
    revision = $snapshot.revision
    listing_count = $listings.Count
    new_snapshots = $newSnapshots
    new_listings = $newListings
  }
}

function Get-MongoListingDocumentsJson {
  if ($SkipMongo) {
    return $null
  }
  if (-not (Test-Path -LiteralPath $ComposeFile)) {
    Write-Warning "Compose file not found: $ComposeFile"
    return $null
  }

  $mongoEval = "const docs=db.getSiblingDB('rpf').listings.find({}).sort({updated_at:-1}).limit($LimitPerPoll).toArray(); print(EJSON.stringify(docs));"

  Push-Location $RepoRoot
  try {
    $raw = & docker compose -f $ComposeFile exec -T mongo mongosh --quiet --eval $mongoEval
    if ($LASTEXITCODE -ne 0) {
      Write-Warning "Mongo sample query failed with exit code $LASTEXITCODE."
      return $null
    }
  }
  finally {
    Pop-Location
  }

  $json = ($raw | Out-String).Trim()
  if ([string]::IsNullOrWhiteSpace($json)) {
    return $null
  }

  return $json
}

function Save-MongoListingSamples {
  $rawJson = Get-MongoListingDocumentsJson
  if ([string]::IsNullOrWhiteSpace($rawJson)) {
    return [pscustomobject]@{
      document_count = 0
      new_documents = 0
    }
  }

  $docs = @($rawJson | ConvertFrom-Json)
  $newDocs = 0
  foreach ($doc in $docs) {
    $metadata = [ordered]@{
      source = "mongo-listings"
    }
    if ($null -ne $doc._id) {
      $metadata["mongo_id"] = ConvertTo-CompactJson $doc._id
    }
    if ($null -ne $doc.listing) {
      $metadata["duty_id"] = $doc.listing.duty
      $metadata["listing_id"] = $doc.listing.id
      $metadata["last_server_restart"] = $doc.listing.last_server_restart
    }

    $saved = Save-JsonSample `
      -Kind "mongo-listing" `
      -Value $doc `
      -Directory $MongoListingDir `
      -Metadata $metadata
    if ($saved) {
      $newDocs++
    }
  }

  return [pscustomobject]@{
    document_count = $docs.Count
    new_documents = $newDocs
  }
}

$stopAt = (Get-Date).AddSeconds([Math]::Max(1, $DurationSeconds))
$poll = 0
$totalNewSnapshots = 0
$totalNewListings = 0
$totalNewMongoDocs = 0

Write-Host "Capturing listing samples from $BaseUrl"
Write-Host "Output: $OutputRoot"
Write-Host "DurationSeconds=$DurationSeconds IntervalSeconds=$IntervalSeconds LimitPerPoll=$LimitPerPoll SkipMongo=$SkipMongo"

while ((Get-Date) -lt $stopAt) {
  $poll++
  try {
    $api = Save-ApiSnapshotSamples
    $mongo = Save-MongoListingSamples

    $totalNewSnapshots += $api.new_snapshots
    $totalNewListings += $api.new_listings
    $totalNewMongoDocs += $mongo.new_documents

    Write-Host (
      "poll={0} revision={1} api_listings={2} new_snapshots={3} new_api_listings={4} mongo_docs={5} new_mongo_docs={6}" -f `
        $poll,
        $api.revision,
        $api.listing_count,
        $api.new_snapshots,
        $api.new_listings,
        $mongo.document_count,
        $mongo.new_documents
    )
  }
  catch {
    Write-Warning "Sample capture poll failed: $($_.Exception.Message)"
  }

  if ((Get-Date) -lt $stopAt) {
    Start-Sleep -Seconds ([Math]::Max(1, $IntervalSeconds))
  }
}

Write-Host (
  "Capture complete. total_new_snapshots={0} total_new_api_listings={1} total_new_mongo_docs={2} manifest={3}" -f `
    $totalNewSnapshots,
    $totalNewListings,
    $totalNewMongoDocs,
    $ManifestPath
)
