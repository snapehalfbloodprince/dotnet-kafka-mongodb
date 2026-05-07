FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["src/KafkaRouter.Worker/KafkaRouter.Worker.csproj", "src/KafkaRouter.Worker/"]
RUN dotnet restore "src/KafkaRouter.Worker/KafkaRouter.Worker.csproj"

COPY . .
WORKDIR "/src/src/KafkaRouter.Worker"

RUN dotnet publish "KafkaRouter.Worker.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "KafkaRouter.Worker.dll"]