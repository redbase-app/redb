# The web server. Build from the solution folder:
#   docker build -f RedbBff.Web/Dockerfile -t redbbff-web .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbBff.Models/RedbBff.Models.csproj RedbBff.Models/
COPY RedbBff.Web/RedbBff.Web.csproj RedbBff.Web/
RUN dotnet restore RedbBff.Web/RedbBff.Web.csproj
COPY . .
RUN dotnet publish RedbBff.Web/RedbBff.Web.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "RedbBff.Web.dll"]
