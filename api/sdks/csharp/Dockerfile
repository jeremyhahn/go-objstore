# Dockerfile for C# SDK testing
FROM mcr.microsoft.com/dotnet/sdk:9.0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Create directory structure and copy proto file
RUN mkdir -p /proto
COPY proto/objstore.proto /proto/

# Copy C# SDK project files first
COPY sdks/csharp/ObjStore.SDK.csproj ./
COPY sdks/csharp/Tests/Tests.csproj ./Tests/

# Restore dependencies
RUN dotnet restore ObjStore.SDK.csproj && \
    dotnet restore Tests/Tests.csproj

# Copy SDK source code (exclude Tests directory)
COPY sdks/csharp/Clients ./Clients/
COPY sdks/csharp/Models ./Models/
COPY sdks/csharp/Exceptions ./Exceptions/
COPY sdks/csharp/Extensions ./Extensions/
COPY sdks/csharp/*.cs ./

# Copy test source code
COPY sdks/csharp/Tests ./Tests/

# Build the tests (this will automatically build the SDK as a dependency)
RUN dotnet build Tests/Tests.csproj --configuration Release

# Default command runs unit tests
CMD ["dotnet", "test", "Tests/Tests.csproj", \
     "--configuration", "Release", \
     "--no-build", \
     "--filter", "FullyQualifiedName~Unit", \
     "--logger", "console;verbosity=detailed"]
