# Changelog

All notable changes to this project will be documented in this file.

## [0.1.4] - 2026-05-07

### Fixed
- Fixed `TypeError` when running scripts without inputs (empty or None inputs handling).
- Improved optional input handling: Sparkit now defaults missing optional inputs to `None` instead of raising an error.
- Added automatic detection of `Optional[...]` type hints to mark inputs as non-required.

### Added
- Comprehensive input validation test suite using `unittest`.
- Robust handling of `stdin` for non-interactive environments.
