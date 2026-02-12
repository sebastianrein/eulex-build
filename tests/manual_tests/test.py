from pathlib import Path

from eulexbuild import EULEXBuildPipeline

if __name__ == '__main__':
    output_dir = Path(__file__).parent / "output"
    if output_dir.exists():
        for file in output_dir.iterdir():
            file.unlink()
        output_dir.rmdir()

    # pipeline = EULEXBuildPipeline(Path(__file__).parent / "my_config.yaml")
    pipeline = EULEXBuildPipeline(Path(__file__).parent / "valid_fixed.yaml")
    pipeline.run()
