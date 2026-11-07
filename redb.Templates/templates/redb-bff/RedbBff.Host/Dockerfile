# The backend in its own host. Build from the solution folder:
#   docker build -f RedbBff.Host/Dockerfile -t redbbff-backend .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbBff.Models/RedbBff.Models.csproj RedbBff.Models/
COPY RedbBff.Backend/RedbBff.Backend.csproj RedbBff.Backend/
COPY RedbBff.Host/RedbBff.Host.csproj RedbBff.Host/
RUN dotnet restore RedbBff.Host/RedbBff.Host.csproj
COPY . .
RUN dotnet publish RedbBff.Host/RedbBff.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
# The database goes to a volume. The app runs as the non-root user of the .NET image, which must be
# able to write there.
RUN mkdir /data && chown $APP_UID /data
USER $APP_UID
ENV ConnectionStrings__Redb="Data Source=/data/redbbff.db"
# Inside a container the backend must listen on all interfaces to be reachable from the web container;
# the compose files do not publish its port.
ENV Tsak__Contexts__redbbff__Override__Api__Host=0.0.0.0
VOLUME /data
EXPOSE 5093
ENTRYPOINT ["dotnet", "RedbBff.Host.dll"]
