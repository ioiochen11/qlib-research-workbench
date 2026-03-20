from __future__ import annotations

import argparse
from pathlib import Path

from .config import AppConfig
from .data_service import DataService
from .qlib_check import run_qlib_smoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal data validator for qlibAssistant.")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config.")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("probe", help="Probe remote mirrors.")
    subparsers.add_parser("status", help="Show local and remote data status.")
    subparsers.add_parser("verify", help="Verify extracted local dataset structure.")
    subparsers.add_parser("qlib-check", help="Initialize qlib and read sample features.")
    sync_parser = subparsers.add_parser("sync-akshare", help="Sync daily bars from AkShare into local qlib data.")
    sync_parser.add_argument("--start-date", default=None, help="Sync start date in YYYY-MM-DD format.")
    sync_parser.add_argument("--end-date", default=None, help="Sync end date in YYYY-MM-DD format.")
    sync_parser.add_argument("--limit", type=int, default=None, help="Only sync the first N symbols.")

    download_parser = subparsers.add_parser("download", help="Download the remote asset.")
    download_parser.add_argument("--output", default=None, help="Output path for the archive.")
    download_parser.add_argument(
        "--max-bytes",
        type=int,
        default=None,
        help="Only download the first N bytes for smoke testing.",
    )

    extract_parser = subparsers.add_parser("extract", help="Extract a downloaded archive.")
    extract_parser.add_argument("--archive", default=None, help="Archive path to extract.")
    extract_parser.add_argument("--target-dir", default=None, help="Extraction target directory.")
    extract_parser.add_argument(
        "--strip-components",
        type=int,
        default=0,
        help="Strip leading path components during extraction.",
    )
    return parser


def cmd_probe(service: DataService) -> int:
    for item in service.probe():
        print(
            f"[{item.mirror_name}] ok={item.ok} status={item.status_code} "
            f"size={item.content_length} last_modified={item.last_modified} url={item.url}"
        )
        if item.error:
            print(f"  error={item.error}")
    return 0


def cmd_status(service: DataService) -> int:
    local_date = service.read_local_calendar_date()
    remote_date = service.remote_publish_date()
    print(f"local_calendar_date={local_date}")
    print(f"remote_publish_date={remote_date}")
    return 0


def cmd_download(service: DataService, output: str | None, max_bytes: int | None) -> int:
    target = output or service.config.download_output
    downloaded = service.download(target, max_bytes=max_bytes)
    print(f"downloaded={downloaded}")
    print(f"size={Path(downloaded).stat().st_size}")
    return 0


def cmd_extract(
    service: DataService,
    archive: str | None,
    target_dir: str | None,
    strip_components: int,
) -> int:
    source = archive or service.config.download_output
    target = target_dir or service.config.extract_dir
    extracted = service.extract_archive(source, target, strip_components=strip_components)
    print(f"extracted_to={extracted}")
    return 0


def cmd_verify(service: DataService) -> int:
    info = service.verify_local_dataset()
    for key, value in info.items():
        print(f"{key}={value}")
    return 0


def cmd_qlib_check(config: AppConfig) -> int:
    info = run_qlib_smoke(config)
    for key in ["provider_uri", "latest_trade_date", "csi300_count", "sample_instrument"]:
        print(f"{key}={info[key]}")
    print("sample_features=")
    print(info["sample_features"])
    return 0


def cmd_sync_akshare(config: AppConfig, start_date: str | None, end_date: str | None, limit: int | None) -> int:
    from .data_cli import DataCLI

    summary = DataCLI(config).sync_akshare(start_date=start_date, end_date=end_date, limit=limit)
    for key, value in summary.items():
        print(f"{key}={value}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = AppConfig.from_yaml(args.config)
    service = DataService(config)

    if args.command == "probe":
        return cmd_probe(service)
    if args.command == "status":
        return cmd_status(service)
    if args.command == "download":
        return cmd_download(service, args.output, args.max_bytes)
    if args.command == "extract":
        return cmd_extract(service, args.archive, args.target_dir, args.strip_components)
    if args.command == "verify":
        return cmd_verify(service)
    if args.command == "qlib-check":
        return cmd_qlib_check(config)
    if args.command == "sync-akshare":
        return cmd_sync_akshare(config, args.start_date, args.end_date, args.limit)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
