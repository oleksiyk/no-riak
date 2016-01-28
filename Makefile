ISTANBUL = ./node_modules/.bin/istanbul
ESLINT = ./node_modules/.bin/eslint
MOCHA = ./node_modules/.bin/mocha

RIAK_PROTO_REV = 2.1.2.0
RIAK_PROTO_BASEURL = https://raw.githubusercontent.com/basho/riak_pb/$(RIAK_PROTO_REV)/src/
RIAK_PROTO_FILES = riak.proto riak_dt.proto riak_kv.proto riak_pb_messages.csv riak_search.proto riak_ts.proto riak_yokozuna.proto

all: lint test coverage

# Tests
test:
	@$(ISTANBUL) cover --report text --report html _mocha

# Check code style
lint:
	@$(ESLINT) lib/**/*.js test/**/*.js

# Check coverage levels
coverage:
	@$(ISTANBUL) check-coverage --statement 85 --branch 70 --function 85

# Clean up
clean: clean-cov

clean-cov:
	@rm -rf coverage

proto:
	@for file in $(RIAK_PROTO_FILES); do echo "$$file" && curl -# -o "src/$$file" "$(RIAK_PROTO_BASEURL)$$file"; done

.PHONY: all test lint coverage clean clean-cov

