
sources := proxy_link.c
sources += proxy_buffer.c
sources += proxy_log.c

proxy_sources := libcephfsd.c
proxy_sources += proxy_manager.c
proxy_sources += $(sources)

lib_sources := libcephfs_proxy.c
lib_sources += $(sources)

test_sources := libcephfsd_test.c

#CFLAGS := -Wall -O0 -g -D_FILE_OFFSET_BITS=64
CFLAGS := -Wall -O3 -flto -D_FILE_OFFSET_BITS=64

.PHONY: all
all:			libcephfsd libcephfsd_test

.PHONY: install
install:		all
			cp -f libcephfs_proxy.so /usr/lib64/

libcephfsd:		$(proxy_sources:.c=.o)
			gcc $(CFLAGS) -o $@ $^ -lcephfs

libcephfsd_test:	$(test_sources:.c=.o) libcephfs_proxy.so
			gcc $(CFLAGS) -L. -o $@ $(test_sources:.c=.o) -lcephfs_proxy

libcephfs_proxy.so:	$(lib_sources:.c=.so.o)
			gcc $(CFLAGS) -fvisibility=hidden -shared -fPIC -o $@ $^

%.so.o:			%.c Makefile
			gcc $(CFLAGS) -fvisibility=hidden -c -fPIC -o $@ $<

%.o:			%.c Makefile
			gcc $(CFLAGS) -c -o $@ $<

.PHONY:	clean
clean:
			rm -f *.o libcephfsd libcephfs_proxy.so libcephfsd_test
