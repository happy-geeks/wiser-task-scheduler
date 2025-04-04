FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["WiserTaskScheduler/WiserTaskScheduler.csproj", "WiserTaskScheduler/"]
RUN dotnet restore "WiserTaskScheduler/WiserTaskScheduler.csproj"
COPY . .
WORKDIR "/src/WiserTaskScheduler"
RUN dotnet build "WiserTaskScheduler.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "WiserTaskScheduler.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "WiserTaskScheduler.dll"]