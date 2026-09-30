# The chat in its own host, without Tsak: the HTTP endpoint, and the console when started with -it.
# Build: docker build -t redbchat .
# Run:   docker run -it -p 5090:5090 -v redbchat-data:/data \
#          -e Tsak__Contexts__redbchat__Override__Llm__ApiKey=YOUR_KEY redbchat
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbChat.Module/RedbChat.Module.csproj RedbChat.Module/
COPY RedbChat.Host/RedbChat.Host.csproj RedbChat.Host/
RUN dotnet restore RedbChat.Host/RedbChat.Host.csproj
COPY . .
RUN dotnet publish RedbChat.Host/RedbChat.Host.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
# The chat history goes to a volume. The app runs as the non-root user of the .NET image, which must
# be able to write there.
RUN mkdir /data && chown $APP_UID /data
USER $APP_UID
ENV ConnectionStrings__Redb="Data Source=/data/redbchat.db"
# Inside a container the endpoint must listen on all interfaces to be reachable through -p.
ENV Tsak__Contexts__redbchat__Override__Http__Host=0.0.0.0
VOLUME /data
EXPOSE 5090
ENTRYPOINT ["dotnet", "RedbChat.Host.dll"]
