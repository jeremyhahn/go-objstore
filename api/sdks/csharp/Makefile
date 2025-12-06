.PHONY: help build test clean coverage integration-test restore format lint docker-build docker-test-unit docker-test-integration docker-test

# Default target
help:
	@echo "Go-ObjStore C# SDK - Available targets:"
	@echo "  build              - Build the SDK and tests"
	@echo "  test               - Run unit tests"
	@echo "  integration-test   - Run integration tests (requires Docker)"
	@echo "  coverage           - Run tests with code coverage"
	@echo "  clean              - Clean build artifacts"
	@echo "  restore            - Restore NuGet packages"
	@echo "  format             - Format code"
	@echo "  lint               - Run code analysis"
	@echo "  all                - Clean, restore, build, and test"

# Restore NuGet packages
restore:
	@echo "Restoring NuGet packages..."
	dotnet restore ObjStore.SDK.csproj
	dotnet restore Tests/Tests.csproj

# Build the SDK and tests
build: restore
	@echo "Building SDK..."
	dotnet build ObjStore.SDK.csproj --configuration Release --no-restore
	@echo "Building tests..."
	dotnet build Tests/Tests.csproj --configuration Release --no-restore

# Run unit tests in Docker
test:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit csharp-unit-tests
	docker compose down -v

# Run integration tests in Docker
integration-test:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server csharp-integration-tests
	docker compose down -v

# Run all tests with code coverage
coverage: build
	@echo "Running tests with code coverage..."
	dotnet test Tests/Tests.csproj \
		--configuration Release \
		--no-build \
		--collect:"XPlat Code Coverage" \
		--results-directory ./coverage \
		--logger "console;verbosity=detailed" \
		/p:CollectCoverage=true \
		/p:CoverletOutputFormat=opencover \
		/p:CoverletOutput=./coverage/ \
		/p:Exclude="[xunit.*]*"
	@echo "Coverage report generated in ./coverage/"
	@echo "To view coverage report, use: reportgenerator or coverlet"

# Clean build artifacts and Docker resources
clean:
	@echo "Cleaning build artifacts..."
	dotnet clean ObjStore.SDK.csproj
	dotnet clean Tests/Tests.csproj
	rm -rf bin obj Tests/bin Tests/obj coverage TestResults
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true

# Format code
format:
	@echo "Formatting code..."
	dotnet format ObjStore.SDK.csproj
	dotnet format Tests/Tests.csproj

# Run code analysis
lint:
	@echo "Running code analysis..."
	dotnet build ObjStore.SDK.csproj /p:EnforceCodeStyleInBuild=true /p:TreatWarningsAsErrors=true

# Build NuGet package
package: build
	@echo "Creating NuGet package..."
	dotnet pack ObjStore.SDK.csproj \
		--configuration Release \
		--no-build \
		--output ./dist

# Run all targets
all: clean restore build test coverage

# Install required tools
install-tools:
	@echo "Installing .NET tools..."
	dotnet tool install --global dotnet-reportgenerator-globaltool || true
	dotnet tool install --global dotnet-format || true

# Generate coverage report (requires reportgenerator)
coverage-report: coverage
	@echo "Generating HTML coverage report..."
	reportgenerator \
		-reports:"./coverage/**/coverage.opencover.xml" \
		-targetdir:"./coverage/report" \
		-reporttypes:"Html;TextSummary"
	@echo "Coverage report generated in ./coverage/report/"
	@echo "Open ./coverage/report/index.html in a browser to view"

docker-build: ## Build Docker test image
	@echo "Building Docker test image..."
	docker compose build csharp-unit-tests

docker-test-unit: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit csharp-unit-tests
	docker compose down -v

docker-test-integration: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server csharp-integration-tests
	docker compose down -v

docker-test: docker-test-unit docker-test-integration ## Run all tests in Docker
