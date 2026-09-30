# Build: docker build -t redbblazor .
# Run:   docker run -p 8080:8080 -v redbblazor-data:/data redbblazor
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY RedbBlazor.csproj .
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
# The SQLite file goes to a volume, so it survives a new container. The app runs as the
# non-root user of the .NET image, which must be able to write there.
RUN mkdir /data && chown $APP_UID /data
USER $APP_UID
ENV ConnectionStrings__Redb="Data Source=/data/app.db"
ENV ASPNETCORE_URLS=http://+:8080
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["dotnet", "RedbBlazor.dll"]
