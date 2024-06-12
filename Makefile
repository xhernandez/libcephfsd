
sources := proxy_link.c
sources += proxy_buffer.c
sources += proxy_log.c

proxy_sources := libcephfsd.c
proxy_sources += proxy_manager.c
proxy_sources += proxy_mount.c
proxy_sources += proxy_helpers.c
proxy_sources += $(sources)

lib_sources := libcephfs_proxy.c
lib_sources += $(sources)

test_sources := libcephfsd_test.c

DAEMON_LIBS := -lcrypto -lcephfs
PROXY_LIBS :=

CFLAGS := -Wall -O0 -g -D_FILE_OFFSET_BITS=64
#CFLAGS := -Wall -O3 -flto -D_FILE_OFFSET_BITS=64

.PHONY: all
all:			libcephfsd libcephfs_proxy.so tests

.PHONY: tests
tests:
			make -C tests

.PHONY: install
install:		lincephfs_proxy.so
			cp -f libcephfs_proxy.so /usr/lib64/

libcephfsd:		$(proxy_sources:.c=.o)
			gcc $(CFLAGS) -o $@ $^ $(DAEMON_LIBS)

libcephfs_proxy.so:	$(lib_sources:.c=.so.o)
			gcc $(CFLAGS) -fvisibility=hidden -shared -fPIC -o $@ $^ $(PROXY_LIBS)

%.so.o:			%.c Makefile
			gcc $(CFLAGS) -fvisibility=hidden -c -fPIC -o $@ $<

%.o:			%.c Makefile
			gcc $(CFLAGS) -c -o $@ $<

.PHONY:	clean
clean:
			rm -f *.o libcephfsd libcephfs_proxy.so
			make -C tests clean
