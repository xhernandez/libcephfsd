
#ifndef __LIBCEPHFSD_PROXY_TESTS_COMMON_H__
#define __LIBCEPHFSD_PROXY_TESTS_COMMON_H__

#include "proxy_log.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>

#include <cephfs/libcephfs.h>

#define CHECK(_err, _func, _args...) \
    do { \
        if (_err >= 0) { \
            _err = _func(_args); \
            printf("#### " #_func "() -> %d\n", _err); \
            if (_err < 0) { \
                printf(#_func "() failed: (%d) %s\n", -_err, strerror(-_err)); \
            } \
        } \
    } while (0)

#define CHECK_PTR(_err, _func, _args...) \
    ({ \
        void *__ptr = NULL; \
        if (_err >= 0) { \
            __ptr = _func(_args); \
            _err = errno; \
            printf("#### " #_func "() -> %p\n", __ptr); \
            if (__ptr == NULL) { \
                printf(#_func "() failed: (%d) %s\n", _err, strerror(_err)); \
                _err = -_err; \
            } \
        } \
        __ptr; \
    })

void
test_init(void);

void
test_done(void);

void
show_statx(const char *text, struct ceph_statx *stx);

#endif
