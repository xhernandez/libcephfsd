
tests := basic

CFLAGS := -Wall -O0 -g -D_FILE_OFFSET_BITS=64
#CFLAGS := -Wall -O3 -flto -D_FILE_OFFSET_BITS=64

MAKEFLAGS += --no-builtin-rules

.PHONY: all
all:			$(tests)

%.o:			%.c Makefile
			gcc $(CFLAGS) -I.. -c -o $@ $<

%:			%.o test_common.o Makefile
			gcc $(CFALGS) -L.. -o $@ $< test_common.o -lcephfs_proxy

.PHONY:	clean
clean:
			rm -f *.o $(tests)
