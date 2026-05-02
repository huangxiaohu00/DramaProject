# Project Structure

## Core Runtime

- `drama_pipeline/`: active Python package for today recommendation and yesterday order workflows.
- `today_recommend_entry.py`: PyInstaller entry for today recommendation.
- `yesterday_orders_entry.py`: PyInstaller entry for yesterday orders.
- `build_drama_pipeline_exes.py`: packaging script for both executable workflows.
- `dist/drama_pipeline_release/`: current packaged executable release.

## Runtime Configuration

- `drama_pipeline/drama_pipeline_config.xlsx`: main user-editable runtime configuration.
- `drama_pipeline/adult_filter.xlsx`: local adult-title filter input.
- `drama_pipeline/material_failed_records.xlsx`: recent failed-material cooldown records.
- `drama_pipeline/basic_data/`: static lookup/reference data used by the pipeline.

## Documentation

- `docs/planning/`: project plans, recommendation scoring plan, implementation plan, and evaluation report.
- `docs/superpowers/`: skill-generated design/plan records.
- `docs/drama_recommendation_scoring.md`: recommendation scoring notes.

## Generated Output

- `drama_pipeline/today_recommend/`: dated today recommendation outputs.
- `drama_pipeline/yesterday_orders/`: dated yesterday order outputs.

## Cleanup Notes

The root directory has been reduced to active packaging/runtime files plus a few Windows-permission-blocked legacy items. Files copied into `docs/planning/` are the canonical planning documents going forward.
