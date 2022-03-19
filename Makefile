## -----------------------------------------------------------------------------
## Sparkles - Spark analyzer and tuning tool
## -----------------------------------------------------------------------------
## Available commands:

NAME=Sparkles
VERSION=0.1.0

help:        ## show help
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

version:     ## check version
	@echo "${NAME} - v${VERSION}"

all:         ## run all build
	@echo "=>> Running all..."
	sbt \
		clean \
		assembly \
		"sparkles/scalafix --check" \
		"sparkles/Test/scalafix --check" \
		sparkles/scalafmtCheck \
		sparkles/Test/scalafmtCheck \
		test

# all: compile lint test package

format:      ## format source code
	@echo "=>> Formating..."
	sbt \
		sparkles/scalafix \
		sparkles/Test/scalafix \
		sparkles/scalafmt \
		sparkles/Test/scalafmt 

lint:        ## lint source code
	@echo "=>> Linting..."
	sbt \
		"sparkles/scalafix --check" \
		"sparkles/Test/scalafix --check" \
		sparkles/scalafmtCheck \
		sparkles/Test/scalafmtCheck
		
compile:     ## compile
	@echo "=>> Compiling..."
	sbt clean compile

test:        ## run tests
	@echo "=>> Testing..."
	sbt test

package:     ## package jar file
	@echo "=>> Packaging..."
	sbt clean package

assembly:    ## package fat jar file
	@echo "=>> Running assembly..."
	sbt clean assembly

doc:         ## generate doc
	@echo "=>> Generating documentation..."
	sbt doc

.PHONY: help version all format lint compile test package assembly doc