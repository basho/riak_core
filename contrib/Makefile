

INC=-I $(CURDIR)/../deps/riak_core/include/ -I $(CURDIR)/../include/
ERLC=erlc

BUILD=$(ERLC) $(INC)

all:
	@echo $(CURDIR)
	$(BUILD) handoff_perftool.erl
