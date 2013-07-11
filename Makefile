APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	public_key mnesia syntax_tools compiler
COMBO_PLT = $(HOME)/.riak_core_combo_dialyzer_plt
PULSE_TESTS = worker_pool_pulse

# The code below figures out the OTP release and introduces a macro at 
# build and test time to tell later released to use the new hash
# functions introduced in R15B02.  Older versions still use the old
# hash functions.
VSN := $(shell erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), init:stop().' | grep 'R' | sed -e 's,R\(..\)B.*,\1,')
NEW_HASH := $(shell expr $(VSN) \>= 16)

.PHONY: deps test

all: deps compile

compile:	
ifeq ($(NEW_HASH),1)
	./rebar compile -Dnew_hash
else
	./rebar compile
endif

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

test: all	
ifeq ($(NEW_HASH),1)
	./rebar skip_deps=true -Dnew_hash eunit
else
	./rebar skip_deps=true eunit
endif

# You should 'clean' before your first run of this target
# so that deps get built with PULSE where needed.
pulse:
	./rebar compile -D PULSE
	./rebar eunit -D PULSE skip_deps=true suite=$(PULSE_TESTS)

docs: deps
	./rebar skip_deps=true doc

build_plt: compile
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

check_plt: compile
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	dialyzer --plt $(COMBO_PLT) ebin


