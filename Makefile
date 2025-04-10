# Makefile for the fsbroker project

# Variables
COVERAGE_FILE=coverage.out
# Assuming 'main' is your primary branch and 'origin' is your remote
GIT_REMOTE=origin
GIT_BRANCH=main

# Default target: runs when 'make' is called without arguments
.DEFAULT_GOAL := help

# Phony targets don't represent files and will always execute
.PHONY: test coverage-func coverage-html release clean help

test: ## Run tests verbosely
	@echo "Running tests..."
	@go test -v ./...

coverage-func: ## Run tests and display function coverage
	@echo "Generating function coverage report..."
	@go test -coverprofile=$(COVERAGE_FILE) ./... > /dev/null 2>&1
	@go tool cover -func=$(COVERAGE_FILE)
	@echo "---"
	@echo "To view HTML report, run: make coverage-html"

coverage-html: ## Run tests and generate an HTML coverage report
	@echo "Generating HTML coverage report..."
	@go test -coverprofile=$(COVERAGE_FILE) ./... > /dev/null 2>&1
	@go tool cover -html=$(COVERAGE_FILE)
	@echo "HTML report generated (usually opens in browser)."

release: test ## Tag a new patch version and push to git remote
	@echo "Starting release process..."
	# 1. Ensure working directory is clean
	@git diff --quiet --exit-code || (echo "ERROR: Uncommitted changes detected. Please commit or stash them." && exit 1)
	@git diff --cached --quiet --exit-code || (echo "ERROR: Changes staged for commit. Please commit or unstage them." && exit 1)
	@echo "Working directory is clean."
	# 2. Ensure we are on the main branch (optional, uncomment to enforce)
	# @[ $$(git rev-parse --abbrev-ref HEAD) = "$(GIT_BRANCH)" ] || (echo "ERROR: Not on $(GIT_BRANCH) branch." && exit 1)
	# 3. Fetch latest tags
	@git fetch $(GIT_REMOTE) --tags
	# 4. Get latest tag, default to v0.0.0 if none exists
	@{ \
		LATEST_TAG=$$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"); \
		echo "Latest tag: $$LATEST_TAG"; \
		\
		# Remove 'v' prefix if present
		VERSION_NUM=$$(echo $$LATEST_TAG | sed 's/^v//'); \
		\
		# Increment patch version (assuming semantic versioning X.Y.Z)
		# Using awk for robustness
		NEW_VERSION_NUM=$$(echo $$VERSION_NUM | awk -F. '{OFS="."; $$NF = $$NF + 1 ; print}'); \
		NEW_TAG="v$$NEW_VERSION_NUM"; \
		\
		echo "Creating new tag: $$NEW_TAG"; \
		git tag $$NEW_TAG || exit 1; \
		\
		echo "Pushing $(GIT_BRANCH) branch and tag $$NEW_TAG to $(GIT_REMOTE)..."; \
		git push $(GIT_REMOTE) $(GIT_BRANCH) || exit 1; \
		git push $(GIT_REMOTE) $$NEW_TAG || exit 1; \
		\
		echo "Release $$NEW_TAG successfully pushed."; \
	}

clean: ## Remove generated files (binary and coverage report)
	@echo "Cleaning up..."
	@rm -f $(BINARY_NAME) $(COVERAGE_FILE)
	@echo "Clean complete."

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
