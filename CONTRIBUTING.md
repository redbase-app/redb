# Contributing to RedBase

Thank you for your interest in RedBase!

## How to Contribute

### Reporting Issues

- Use [GitHub Issues](https://github.com/redbase-app/redb/issues) for bug reports
- Use [GitHub Discussions](https://github.com/redbase-app/redb/discussions) for questions and feature requests

### Bug Reports

Please include:
- RedBase package version (`redb.Core`, provider package)
- .NET version
- Database type and version (PostgreSQL / MSSQL)
- Minimal code to reproduce the issue
- Expected vs actual behavior

### Feature Requests

Open a [Discussion](https://github.com/redbase-app/redb/discussions/categories/ideas) with:
- Description of the feature
- Use case / motivation
- Example code showing desired API (if applicable)

## Code Contributions

This repository contains **examples, CLI tool, and export library**. The core packages (`redb.Core`, `redb.Postgres`, `redb.MSSql`) are developed in a separate repository.

If you'd like to contribute examples:

1. Fork the repository
2. Create a branch: `git checkout -b my-example`
3. Add your example following the existing pattern in `redb.Examples/Examples/`
4. Submit a Pull Request

### Example Guidelines

- Inherit from `ExampleBase` and add `[ExampleMeta]` attribute
- Use `EmployeeProps` or other models from `Models/ExampleModels.cs`
- Keep examples focused on one feature
- Include clear comments explaining what the example demonstrates

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
