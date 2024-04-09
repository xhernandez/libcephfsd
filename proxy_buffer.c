
#include <stdio.h>

#include "proxy_buffer.h"
#include "proxy_helpers.h"

static int32_t
proxy_buffer_op_write(proxy_buffer_t *buffer, void *ptr, int32_t size)
{
    if (buffer->ops->write != NULL) {
        return buffer->ops->write(buffer, ptr, size);
    }

    return proxy_log(LOG_ERR, EIO, "Unable to write buffer");
}

static int32_t
proxy_buffer_op_overflow(proxy_buffer_t *buffer, int32_t size)
{
    if (buffer->ops->overflow != NULL) {
        return buffer->ops->overflow(buffer, size);
    }

    return proxy_log(LOG_ERR, EOVERFLOW, "Buffer overflow");
}

static int32_t
proxy_buffer_op_read(proxy_buffer_t *buffer, void *ptr, int32_t size)
{
    if (buffer->ops->read != NULL) {
        return buffer->ops->read(buffer, ptr, size);
    }

    return 0;
}

int32_t
proxy_buffer_open(proxy_buffer_t *buffer, proxy_buffer_ops_t *ops, void *data,
                  int32_t size, uint32_t mode)
{
    buffer->flags = mode;

    if (data == NULL) {
        data = proxy_malloc(size);
        if (data == NULL) {
            return -ENOMEM;
        }
        buffer->flags |= BUFFER_ALLOCATED;
    }

    buffer->ops = ops;
    buffer->data = data;
    buffer->size = size;
    buffer->available = (mode & BUFFER_WRITE) == 0 ? 0 : size;
    buffer->pos = 0;

    return 0;
}

int32_t
proxy_buffer_flush(proxy_buffer_t *buffer)
{
    void *ptr;
    int32_t size, err;

    if ((buffer->flags & BUFFER_WRITE) == 0) {
        proxy_abort(0, "Trying to flush to a read-only buffer");
    }

    ptr = buffer->data;
    size = buffer->pos;
    while (size > 0) {
        err = proxy_buffer_op_write(buffer, ptr, size);
        if (err < 0) {
            return err;
        }
        if (err == 0) {
            return proxy_log(LOG_ERR, 0, "Unable to write buffer data");
        }
        ptr += err;
        size -= err;
    }

    buffer->pos = 0;
    buffer->available = buffer->size;

    return buffer->available;
}

int32_t
proxy_buffer_close(proxy_buffer_t *buffer)
{
    int32_t err;

    err = 0;
    if ((buffer->flags & BUFFER_WRITE) != 0) {
        err = proxy_buffer_flush(buffer);
    }

    if ((buffer->flags & BUFFER_ALLOCATED) != 0) {
        proxy_free(buffer->data);
    }

    return err;
}

static int32_t
proxy_buffer_load(proxy_buffer_t *buffer, int32_t size)
{
    void *ptr;
    int32_t max, err;

    ptr = buffer->data + buffer->pos + buffer->available;

    max = buffer->size - buffer->pos;
    if (size > max) {
        memmove(buffer->data, ptr, max - buffer->available);
        buffer->pos = 0;
        ptr = buffer->data;
    }

    size -= buffer->available;
    while (size > 0) {
        err = proxy_buffer_op_read(buffer, ptr, max - buffer->available);
        if (err < 0) {
            return err;
        }
        if (err == 0) {
            break;
        }
        ptr += err;
        buffer->available += err;
        size -= err;
    }

    return buffer->available;
}

static int32_t
proxy_buffer_get(proxy_buffer_t *buffer, int32_t size)
{
    if (size > buffer->size) {
        return proxy_log(LOG_ERR, ENOBUFS, "Requested data space is too long");
    }

    if ((size <= buffer->available) || ((buffer->flags & BUFFER_FIXED) != 0)) {
        return buffer->available;
    }

    if ((buffer->flags & BUFFER_READ) != 0) {
        return proxy_buffer_load(buffer, size);
    }

    return proxy_buffer_flush(buffer);
}

int32_t
proxy_buffer_write(proxy_buffer_t *buffer, const void *data, int32_t size)
{
    int32_t max;

    if ((buffer->flags & BUFFER_WRITE) == 0) {
        proxy_abort(0, "Trying to write to a read-only buffer");
    }

    max = proxy_buffer_get(buffer, size);
    if (max < 0) {
        return max;
    }
    if (max > size) {
        max = size;
    }
    memcpy(buffer->data + buffer->pos, data, max);
    buffer->pos += max;
    buffer->available -= max;

    if (max < size) {
        return proxy_buffer_op_overflow(buffer, max);
    }

    return max;
}

int32_t
proxy_buffer_write_string(proxy_buffer_t *buffer, const char *text)
{
    return proxy_buffer_write(buffer, text, strlen(text) + 1);
}

static int32_t
proxy_buffer_write_error(proxy_buffer_t *buffer, const char *error,
                         const char *text)
{
    int32_t err;

    err = proxy_buffer_write(buffer, "<", 1);
    if (err >= 0) {
        err = proxy_buffer_write_string(buffer, error);
    }
    if ((err >= 0) && (text != NULL)) {
        err = proxy_buffer_write(buffer, " ", 1);
        if (err >= 0) {
            err = proxy_buffer_write_string(buffer, text);
        }
    }
    if (err >= 0) {
        err = proxy_buffer_write(buffer, ">", 1);
    }

    return err;
}

int32_t
proxy_buffer_write_format_args(proxy_buffer_t *buffer, const char *fmt,
                               va_list args)
{
    va_list copy;
    int32_t len, max, err;

    if ((buffer->flags & BUFFER_WRITE) == 0) {
        proxy_abort(0, "Trying to write to a read-only buffer");
    }

    max = buffer->available;

    va_copy(copy, args);
    len = vsnprintf(buffer->data + buffer->pos, max, fmt, copy);
    va_end(copy);

    if (len >= max) {
        err = proxy_buffer_get(buffer, len);
        if (err < 0) {
            return err;
        }

        max = err;

        va_copy(copy, args);
        len = vsnprintf(buffer->data + buffer->pos, max, fmt, copy);
        va_end(args);

    }
    if (len < 0) {
        return proxy_buffer_write_error(buffer, "format_error", fmt);
    }

    if (len >= max) {
        buffer->pos += max;
        buffer->available -= max;

        return proxy_buffer_op_overflow(buffer, max);
    }

    buffer->pos += len;
    buffer->available -= len;

    return len;
}

int32_t
proxy_buffer_write_format(proxy_buffer_t *buffer, const char *fmt, ...)
{
    va_list args;
    int32_t err;

    va_start(args, fmt);
    err = proxy_buffer_write_format_args(buffer, fmt, args);
    va_end(args);

    return err;
}

int32_t
proxy_buffer_read(proxy_buffer_t *buffer, void **pdata, int32_t size)
{
    int32_t max;

    if ((buffer->flags & BUFFER_READ) == 0) {
        proxy_abort(0, "Trying to read from a write-only buffer");
    }

    max = proxy_buffer_get(buffer, size);
    if (max <= 0) {
        return max;
    }
    if (max < size) {
        return proxy_log(LOG_ERR, ENODATA, "Truncated data");
    }

    *pdata = buffer->data + buffer->pos;
    buffer->pos += size;
    buffer->available -= size;

    return size;
}

int32_t
proxy_buffer_read_line(proxy_buffer_t *buffer, char **pline)
{
    char *ptr, *end;
    int32_t len, err;
    bool ignore;

    do {
        ignore = false;
        ptr = buffer->data + buffer->pos;
        while ((end = memchr(ptr, '\n', buffer->available)) == NULL) {
            if (buffer->available == buffer->size) {
                buffer->pos = 0;
                buffer->available = 0;
                ignore = true;
            }
            err = proxy_buffer_load(buffer, 1);
            if (err < 0) {
                return err;
            }

            ptr = buffer->data + buffer->pos;
        }

        len = end - ptr;
        buffer->pos += len + 1;
        buffer->available -= len + 1;

        if (ignore) {
            proxy_log(LOG_ERR, ERANGE, "Ignoring line too long");
        }
    } while (ignore);

    *end = 0;

    *pline = ptr;

    return len;
}
