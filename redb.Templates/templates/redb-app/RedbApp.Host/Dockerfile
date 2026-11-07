# The API in its own host. Build from the solution folder:
#   docker build -f RedbApp.Host/Dockerfile -t redbapp-api .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbApp.Models/RedbApp.Models.csproj RedbApp.Models/
COPY RedbApp.Api/RedbApp.Api.csproj RedbApp.Api/
COPY RedbApp.Host/RedbApp.Host.csproj RedbApp.Host/
RUN dotnet restore RedbApp.Host/RedbApp.Host.csproj
COPY . .
RUN dotnet publish RedbApp.Host/RedbApp.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
# The database goes to a volume. The app runs as the non-root user of the .NET image, which must be
# able to write there.
RUN mkdir /data && chown $APP_UID /data
USER $APP_UID
ENV ConnectionStrings__Redb="Data Source=/data/redbapp.db"
VOLUME /data
EXPOSE 5092
ENTRYPOINT ["dotnet", "RedbApp.Host.dll"]
