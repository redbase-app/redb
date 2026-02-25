# Contributing to RedBase

Thank you for considering contributing to RedBase!

## Code of Conduct

We follow the [.NET Foundation Code of Conduct](https://dotnetfoundation.org/about/policies/code-of-conduct). Be respectful and constructive.

## How to Contribute

### Reporting Bugs

- Use the [bug report template](https://github.com/redbase-app/redb/issues/new?template=bug_report.md)
- Include: steps to reproduce, expected vs actual behavior, .NET version, database engine (PostgreSQL / SQL Server), minimal repro code
- Include the RedBase package version (`redb.Core`, provider package)

### Suggesting Features

- Open a [Discussion](https://github.com/redbase-app/redb/discussions/categories/ideas) with:
  - Description of the feature
  - Use case / motivation
  - Example code showing desired API (if applicable)

### Pull Requests

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests (xUnit preferred)
3. Ensure the code builds: `dotnet build`
4. Ensure all tests pass: `dotnet test`
5. Update `CHANGELOG.md` if needed
6. Open PR with clear title and description
7. Target branch: `main`

## Areas Where We Welcome Contributions

- **Examples** — new examples in `redb.Examples/Examples/` (follow the `ExampleBase` pattern)
- **Documentation** — improvements to README, inline XML docs, tutorials
- **Bug fixes** — especially with a minimal repro
- **Templates** — new `dotnet new` templates in `redb.Templates/`
- **Export formats** — new export targets in `redb.Export/`
- **Future providers** — MySQL, SQLite, Oracle are on the roadmap; reach out in Discussions before starting

The core packages (`redb.Core`, `redb.Postgres`, `redb.MSSql`) are developed in a separate private repository.
If you'd like to contribute to core — open a Discussion and we'll coordinate.

## Development Setup

```bash
git clone https://github.com/redbase-app/redb.git
cd redb
dotnet restore
dotnet build
```

- **IDE**: Visual Studio 2022 or JetBrains Rider
- **SDK**: .NET 8+ (9 or 10 recommended)
- **Database**: PostgreSQL 14+ or SQL Server 2019+
- **Run examples**: `dotnet run --project redb.Examples`

## Coding Guidelines

- C# 12+, nullable reference types enabled
- 4 spaces indentation, no tabs
- Use meaningful names, document public APIs with `/// <summary>` XML comments
- Prefer LINQ over nested loops
- Avoid `dynamic` types — use strong typing
- Follow existing code style in the project

## Example Guidelines

When adding examples to `redb.Examples/Examples/`:

- Inherit from `ExampleBase` and add `[ExampleMeta]` attribute
- Use `EmployeeProps` or other models from `Models/ExampleModels.cs`
- Keep examples focused on one feature
- Include clear comments explaining what the example demonstrates

## Questions?

Open a [Discussion](https://github.com/redbase-app/redb/discussions) or check the docs at [redbase.app](https://redbase.app).

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
