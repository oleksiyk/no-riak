ISTANBUL = ./node_modules/.bin/istanbul
ESLINT = ./node_modules/.bin/eslint
MOCHA = ./node_modules/.bin/mocha

RIAK_PROTO_REV = 2.3.2
RIAK_PROTO_BASEURL = https://raw.githubusercontent.com/basho/riak_pb/$(RIAK_PROTO_REV)/src/
RIAK_PROTO_FILES = riak.proto riak_dt.proto riak_kv.proto riak_pb_messages.csv riak_search.proto riak_ts.proto riak_yokozuna.proto

ifeq ($(RIAK_ADMIN_USE_SUDO),true)
	RIAK_ADMIN = sudo riak-admin
else
	RIAK_ADMIN := riak-admin
endif

all: lint test coverage

# Tests
test: dt-setup
	@$(ISTANBUL) cover --report lcov --report text --report html _mocha

# Check code style
lint:
	@$(ESLINT) 'lib/**/*.js' 'test/**/*.js'

# Check coverage levels
coverage:
	@$(ISTANBUL) check-coverage --statement 85 --branch 70 --function 85

# Clean up
clean: clean-cov

clean-cov:
	@rm -rf coverage

proto:
	@for file in $(RIAK_PROTO_FILES); do echo "$$file" && curl -# -o "src/$$file" "$(RIAK_PROTO_BASEURL)$$file"; done

dt-setup:
	@$(RIAK_ADMIN) bucket-type create no_riak_test_bucket_type '{"props":{"r":1,"allow_mult":true}}' > /dev/null || true
	@$(RIAK_ADMIN) bucket-type create no_riak_test_crdt_counter '{"props":{"datatype":"counter","allow_mult":true}}' > /dev/null || true
	@$(RIAK_ADMIN) bucket-type create no_riak_test_crdt_map '{"props":{"datatype":"map","allow_mult":true}}' > /dev/null || true
	@$(RIAK_ADMIN) bucket-type create no_riak_test_crdt_set '{"props":{"datatype":"set","allow_mult":true}}' > /dev/null || true
	@$(RIAK_ADMIN) bucket-type activate no_riak_test_bucket_type > /dev/null || true
	@$(RIAK_ADMIN) bucket-type activate no_riak_test_crdt_counter > /dev/null || true
	@$(RIAK_ADMIN) bucket-type activate no_riak_test_crdt_map > /dev/null || true
	@$(RIAK_ADMIN) bucket-type activate no_riak_test_crdt_set > /dev/null || true

enable-security:
	@$(RIAK_ADMIN) security enable > /dev/null
	@$(RIAK_ADMIN) security add-user no-riak password=secret > /dev/null
	@$(RIAK_ADMIN) security grant riak_kv.put,riak_kv.get on any to no-riak > /dev/null
	@$(RIAK_ADMIN) security add-source no-riak 127.0.0.1/32 password > /dev/null

disable-security:
	@$(RIAK_ADMIN) security del-user no-riak > /dev/null
	@$(RIAK_ADMIN) security del-source no-riak 127.0.0.1/32 > /dev/null
	@$(RIAK_ADMIN) security disable > /dev/null

.PHONY: all test lint coverage clean clean-cov proto dt-setup enable-security disable-security

