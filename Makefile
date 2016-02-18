DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	public_key mnesia syntax_tools compiler
PULSE_TESTS = worker_pool_pulse

.PHONY: deps test

all: compile

compile: deps
	./rebar3 compile

clean:
	./rebar3 clean

distclean: clean

# You should 'clean' before your first run of this target
# so that deps get built with PULSE where needed.
pulse:
	./rebar compile -D PULSE
	./rebar eunit -D PULSE skip_deps=true suite=$(PULSE_TESTS)

include tools.mk
