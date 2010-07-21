

.PHONY: deps

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean 
	./rebar delete-deps

eunit:
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc
	@cp -R apps/luke/doc doc/luke
	@cp -R apps/riak_core/doc doc/riak_core
	@cp -R apps/riak_kv/doc doc/riak_kv

dialyzer: compile
	@dialyzer -Wno_return -c apps/riak_core/ebin


