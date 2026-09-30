# The worker in its own host, without Tsak.
# Build: docker build -t redbworker .
# Run:   docker run -v redbworker-data:/data redbworker
#        then put order files into the volume's inbox folder.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbWorker.Module/RedbWorker.Module.csproj RedbWorker.Module/
COPY RedbWorker.Host/RedbWorker.Host.csproj RedbWorker.Host/
RUN dotnet restore RedbWorker.Host/RedbWorker.Host.csproj
COPY . .
RUN dotnet publish RedbWorker.Host/RedbWorker.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
# The database and the folders go to a volume. The app runs as the non-root user of the .NET image,
# which must be able to write there.
RUN mkdir /data && chown $APP_UID /data
USER $APP_UID
ENV ConnectionStrings__Redb="Data Source=/data/redbworker.db"
ENV Tsak__Contexts__redbworker__Override__Folders__Inbox=/data/inbox
ENV Tsak__Contexts__redbworker__Override__Folders__Outbox=/data/outbox
ENV Tsak__Contexts__redbworker__Override__Folders__Archive=/data/archive
ENV Tsak__Contexts__redbworker__Override__Folders__Error=/data/error
VOLUME /data
ENTRYPOINT ["dotnet", "RedbWorker.Host.dll"]
