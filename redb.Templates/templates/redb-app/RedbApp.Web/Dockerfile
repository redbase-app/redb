# The client: published once, served by nginx. Build from the solution folder:
#   docker build -f RedbApp.Web/Dockerfile -t redbapp-web .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY RedbApp.Models/RedbApp.Models.csproj RedbApp.Models/
COPY RedbApp.Web/RedbApp.Web.csproj RedbApp.Web/
RUN dotnet restore RedbApp.Web/RedbApp.Web.csproj
COPY . .
RUN dotnet publish RedbApp.Web/RedbApp.Web.csproj -c Release -o /publish --no-restore

FROM nginx:alpine
COPY --from=build /publish/wwwroot /usr/share/nginx/html
COPY RedbApp.Web/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
