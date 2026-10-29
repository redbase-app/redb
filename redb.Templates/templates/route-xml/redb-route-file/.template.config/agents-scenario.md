Files (`redb.Route.File`, `redb.Route.Ftp` or `redb.Route.Sftp`, chosen at `dotnet new` by `--transport`),
`routes/files.route.xml`: a feed route writes test files into `{{files.inbox:inbox}}` (delete it in a real module);
the inbox route polls `*.xml` (`antInclude`), checks each against `resources/order.xsd`, moves a finished file to
`done/` and a failed one to `error/` (`moveTo`, `moveFailed`), and writes a receipt into `{{files.outbox:outbox}}`.
For FTP and SFTP the bean `remote` in `context.xml` holds the server and the credentials; every URI says
`connectionFactory=remote`.

Facts to keep: `moveTo`, `moveFailed`, `preMove` and `doneFileName` take only file variables (`${file:name}`,
`${file:name.noext}`); the producer's `fileName` takes the full expression language; the header prefix is the
transport's (`redbFile`, `redbFtp` or `redbSftp`, then `.Name`, `.NameOnly`, `.Length`); the producer refuses paths outside
its folder; `files.password` is a required key from the environment.

Usually changed: the folders, the include pattern, a read lock for files still being written (`readLock`), a done
marker (`doneFileName`), what the route does with a file.

`routes/CsvFilesRoute.cs` runs CSV files in C# through its own inbox `files.csInbox` (`inbox-cs`) with the same
`done/`, `error/` and outbox; the connection option is the constant `Connection` (empty for local files). Never
point a C# consumer at `files.inbox`: the XML reader owns it.
