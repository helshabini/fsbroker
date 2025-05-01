# Makefile for the fsbroker project

# Variables
COVERAGE_OUT = coverage.out

.PHONY: test coverage clean

test: ## Run tests verbosely
	@echo "Cleaning test cache..."
	@go clean -testcache
	@echo "Running tests..."
	@go test $(if $(debug),-v -tags=fsbroker_debug,) $(if $(run),-run=$(run),) -count=1 ./...

coverage: ## Run tests and display function coverage
	@echo "Generating coverage report..."
	@go test -coverprofile=$(COVERAGE_OUT) ./... > /dev/null 2>&1
	@go tool cover -func=$(COVERAGE_OUT)
	@go tool cover -html=$(COVERAGE_OUT)
	@echo "HTML report generated (usually opens in browser)."

clean: ## Clean up build artifacts
	@rm -f $(COVERAGE_OUT)
	@rm -rf bin