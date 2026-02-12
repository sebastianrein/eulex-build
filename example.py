"""
Example script demonstrating how to use the EULEX-Build pipeline.
"""
from pathlib import Path

from eulexbuild import EULEXBuildPipeline

# Path to your configuration file
CONFIG_PATH = "configuration.yaml"

# Optional: specify a custom database name
DB_NAME = "eulex_build.db"


def main():
    """Run the EULEX-Build pipeline."""
    print("=" * 60)
    print("EULEX-Build Pipeline Example")
    print("=" * 60)

    try:
        # Initialize the pipeline with your configuration
        pipeline = EULEXBuildPipeline(
            config_path=CONFIG_PATH,
            db_name=DB_NAME
        )

        # Run the pipeline
        pipeline.run()

        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        print("\nYour dataset is ready in the output directory.")
        print("Check the generated README.md file for details on how to use the data.")

    except FileNotFoundError as e:
        print(f"Error: Configuration file not found: {e}")
    except Exception as e:
        print(f"Error: Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Clean up output directory of previous runs before running the pipeline
    output_dir = Path(__file__).parent / "output"
    if output_dir.exists():
        for file in output_dir.iterdir():
            file.unlink()
        output_dir.rmdir()

    main()
