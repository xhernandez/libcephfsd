
#ifndef __LIBCEPHFSD_PROXY_BUFFER_H__
#define __LIBCEPHFSD_PROXY_BUFFER_H__

#include "proxy.h"

enum {
    BUFFER_READ = 0x01,
    BUFFER_WRITE = 0x02,
    BUFFER_FIXED = 0x40,
    BUFFER_ALLOCATED = 0x80
};

struct _proxy_buffer_ops {
    int32_t (*read)(proxy_buffer_t *, void *, int32_t);
    int32_t (*write)(proxy_buffer_t *, void *, int32_t);
    int32_t (*overflow)(proxy_buffer_t *, int32_t);
};

struct _proxy_buffer {
    proxy_buffer_ops_t *ops;
    void *data;
    int32_t size;
    int32_t available;
    int32_t pos;
    uint32_t flags;
};

int32_t
proxy_buffer_open(proxy_buffer_t *buffer, proxy_buffer_ops_t *ops, void *data,
                  int32_t size, uint32_t mode);

int32_t
proxy_buffer_flush(proxy_buffer_t *buffer);

int32_t
proxy_buffer_close(proxy_buffer_t *buffer);

int32_t
proxy_buffer_write(proxy_buffer_t *buffer, const void *data, int32_t size);

int32_t
proxy_buffer_write_string(proxy_buffer_t *buffer, const char *text);

int32_t
proxy_buffer_write_format_args(proxy_buffer_t *buffer, const char *fmt,
                               va_list args);

int32_t
proxy_buffer_write_format(proxy_buffer_t *buffer, const char *fmt, ...);

int32_t
proxy_buffer_read(proxy_buffer_t *buffer, void **pdata, int32_t size);

int32_t
proxy_buffer_read_line(proxy_buffer_t *buffer, char **pline);

#endif
