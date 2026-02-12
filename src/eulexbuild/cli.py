"""Command-line interface for EULEX-Build Pipeline."""
import argparse
import sys
from pathlib import Path

from eulexbuild import EULEXBuildPipeline, __version__


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="eulexbuild",
        description="EULEX-Build: Build research-ready datasets from EU legislation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  eulexbuild run config.yaml
  eulexbuild run config.yaml --db-name my_dataset.db
  eulexbuild --version

For more information, visit: https://github.com/yourusername/eulex-build
        """
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"eulexbuild {__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser(
        "run",
        help="Run the EULEX-Build pipeline with a configuration file"
    )
    run_parser.add_argument(
        "config",
        type=str,
        help="Path to the YAML configuration file"
    )
    run_parser.add_argument(
        "--db-name",
        type=str,
        default="eulex_build.db",
        help="Name of the SQLite database file (default: eulex_build.db)"
    )

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if args.command == "run":
        try:
            config_path = Path(args.config)
            if not config_path.exists():
                print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
                sys.exit(1)

            pipeline = EULEXBuildPipeline(config_path, db_name=args.db_name)
            pipeline.run()

            sys.exit(0)

        except Exception as e:
            print(f"Error: Pipeline failed with exception: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
