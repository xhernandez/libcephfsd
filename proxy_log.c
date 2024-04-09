
#include <stdio.h>
#include <stdarg.h>

#include "proxy_log.h"
#include "proxy_buffer.h"
#include "proxy_helpers.h"
#include "proxy_list.h"

#define PROXY_LOG_BUFFER_SIZE 4096

typedef struct _proxy_log_buffer {
    proxy_buffer_t buffer;
    int32_t level;
    int32_t error;
} proxy_log_buffer_t;

static __thread char proxy_log_buffer[PROXY_LOG_BUFFER_SIZE];

static pthread_rwlock_t proxy_log_mutex = PTHREAD_RWLOCK_INITIALIZER;
static list_t proxy_log_handlers = LIST_INIT(&proxy_log_handlers);

static void
proxy_log_write(int32_t level, int32_t err, const char *msg)
{
    proxy_log_handler_t *handler;

    proxy_rwmutex_rdlock(&proxy_log_mutex);

    list_for_each_entry(handler, &proxy_log_handlers, list) {
        handler->callback(handler, level, err, msg);
    }

    proxy_rwmutex_unlock(&proxy_log_mutex);
}

__public void
proxy_log_register(proxy_log_handler_t *handler, proxy_log_callback_t callback)
{
    handler->callback = callback;

    proxy_rwmutex_wrlock(&proxy_log_mutex);

    list_add_tail(&handler->list, &proxy_log_handlers);

    proxy_rwmutex_unlock(&proxy_log_mutex);
}

__public void
proxy_log_deregister(proxy_log_handler_t *handler)
{
    proxy_rwmutex_wrlock(&proxy_log_mutex);

    list_del_init(&handler->list);

    proxy_rwmutex_unlock(&proxy_log_mutex);
}

static int32_t
log_buffer_write(proxy_buffer_t *buffer, void *ptr, int32_t size)
{
    proxy_log_buffer_t *log;

    log = container_of(buffer, proxy_log_buffer_t, buffer);

    proxy_log_write(log->level, log->error, ptr);

    return size;
}

static int32_t
log_buffer_overflow(proxy_buffer_t *buffer, int32_t size)
{
    memcpy(buffer->data + buffer->size - 6, "[...]", 6);
    buffer->pos = buffer->size;

    return size;
}

static proxy_buffer_ops_t log_buffer_ops = {
    .write = log_buffer_write,
    .overflow = log_buffer_overflow
};

int32_t
proxy_log_args(int32_t level, int32_t err, const char *fmt, va_list args)
{
    static __thread bool busy = false;

    proxy_log_buffer_t log;

    if (busy) {
        return -err;
    }
    busy = true;

    log.level = level;
    log.error = err;

    if (proxy_buffer_open(&log.buffer, &log_buffer_ops, proxy_log_buffer,
                          sizeof(proxy_log_buffer),
                          BUFFER_WRITE | BUFFER_FIXED) >= 0) {
        proxy_buffer_write_format_args(&log.buffer, fmt, args);

        if (err != 0) {
            proxy_buffer_write_format(&log.buffer, " (%d) %s", err,
                                      strerror(err));
        }

        proxy_buffer_close(&log.buffer);
    }

    busy = false;

    return -err;
}

int32_t
proxy_log(int32_t level, int32_t err, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    err = proxy_log_args(level, err, fmt, args);
    va_end(args);

    return err;
}

void
proxy_abort_args(int32_t err, const char *fmt, va_list args)
{
    proxy_log_args(LOG_CRIT, err, fmt, args);
    abort();
}

void
proxy_abort(int32_t err, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    proxy_abort_args(err, fmt, args);
    va_end(args);
}
