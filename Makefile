DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	public_key mnesia syntax_tools compiler
PULSE_TESTS = worker_pool_pulse

.PHONY: deps test

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

# You should 'clean' before your first run of this target
# so that deps get built with PULSE where needed.
pulse:
	./rebar compile -D PULSE
	./rebar eunit -D PULSE skip_deps=true suites=$(PULSE_TESTS)

include tools.mk
