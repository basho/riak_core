.PHONY: compile rel cover test dialyzer eqc
REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

cover: 
	$(REBAR) eunit --cover
	$(REBAR) cover

test: compile
	$(REBAR) eunit

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

eqc:
	$(REBAR) as test eqc --testing_budget 120
	$(REBAR) as eqc eunit

check: test dialyzer xref
