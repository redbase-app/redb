# redb.PropsEditor

Blazor component library for editing REDB object properties. Built with [Fluent UI Blazor](https://www.fluentui-blazor.net/).

[![License: MIT](https://img.shields.io/badge/license-MIT-green)](../LICENSE)

## Components

| Component | Description |
|-----------|-------------|
| `PropsEditor` | Full property editor for a `RedbObject<T>` |
| `PropsGrid` | Grid view of Props fields with inline editing |
| `RedbObjectEditor` | Editor for base object fields (Name, ValueString, etc.) |
| `SchemeBrowser` | Browse and select REDB schemes |
| `PropertyField` | Single property field editor (auto-selects input type) |
| `ArrayEditor` | Editor for array/list properties |
| `JsonEditor` | Raw JSON editor for complex properties |
| `NestedObjectEditor` | Editor for nested object properties |
| `NestedArrayEditor` | Editor for arrays of nested objects |

## Installation

Add a project reference:

```xml
<ProjectReference Include="..\redb.PropsEditor\redb.PropsEditor.csproj" />
```

Add to `_Imports.razor`:

```razor
@using redb.PropsEditor.Components
```

## Dependencies

- `redb.Core`
- `Microsoft.AspNetCore.Components.Web`
- `Microsoft.FluentUI.AspNetCore.Components`

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

MIT
