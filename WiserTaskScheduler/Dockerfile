# Stage 1: Build the application
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app
COPY ["WiserTaskScheduler/WiserTaskScheduler.csproj", "./"]

# Copy the rest of the files to the container.
COPY . .

# Build the application and publish it to the working directory.
RUN dotnet publish -c Release -r linux-x64 -o /app/publish --self-contained

# Stage 2: Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# Copy the published files from the build stage to the container.
COPY --from=build /app/publish .

# Install dependencies (combine commands to reduce layers).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libasound2 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 \
    libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 \
    libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcursor1 libxdamage1 \
    libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 libnss3 libgbm1 \
    xvfb x11-apps x11-xkb-utils && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Windows Core Fonts
RUN apt-get update && apt-get install -y wget && \
    wget http://ftp.de.debian.org/debian/pool/contrib/m/msttcorefonts/ttf-mscorefonts-installer_3.8_all.deb && \
    apt-get install -y ./ttf-mscorefonts-installer_3.8_all.deb && \
    fc-cache -f -v && \
    rm -f ./ttf-mscorefonts-installer_3.8_all.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the DISPLAY environment variable for Xvfb
ENV DISPLAY=:99

RUN chmod +x /app/evopdf_loadhtml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]