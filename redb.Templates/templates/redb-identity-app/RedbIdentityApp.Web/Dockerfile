# The web server. Build from the solution folder:
#   docker build -f RedbIdentityApp.Web/Dockerfile -t redbidentityapp-web .
ARG DOTNET_VERSION=10.0
# The Identity image of the same release: the web server trusts its development certificate.
ARG IDENTITY_IMAGE=ghcr.io/redbase-app/redb-identity-full:4.5.0

FROM ${IDENTITY_IMAGE} AS identity

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbIdentityApp.Contracts/RedbIdentityApp.Contracts.csproj RedbIdentityApp.Contracts/
COPY RedbIdentityApp.Web/RedbIdentityApp.Web.csproj RedbIdentityApp.Web/
RUN dotnet restore RedbIdentityApp.Web/RedbIdentityApp.Web.csproj
COPY . .
RUN dotnet publish RedbIdentityApp.Web/RedbIdentityApp.Web.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION}
WORKDIR /app
# Identity's self-signed development certificate, trusted through the CA store: the certificate is
# checked as any other, nothing is switched off. With a real certificate on Identity, delete these two
# lines and the identity stage above.
COPY --from=identity /usr/local/share/ca-certificates/redb-identity-dev.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
COPY --from=build /app .
# The DataProtection key ring goes to a volume. The app runs as the non-root user of the .NET image,
# which must be able to write there.
RUN mkdir /keys && chown $APP_UID /keys
USER $APP_UID
ENV ASPNETCORE_URLS=http://+:5085
ENV Web__DataProtectionKeysPath=/keys
VOLUME /keys
EXPOSE 5085
ENTRYPOINT ["dotnet", "RedbIdentityApp.Web.dll"]
