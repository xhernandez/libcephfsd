
#ifndef __LIBCEPHFS_PROXY_HELPERS_H__
#define __LIBCEPHFS_PROXY_HELPERS_H__

#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>

#include "proxy_log.h"

#define __public __attribute__((__visibility__("default")))

#define ptr_value(_ptr) ((uint64_t)(uintptr_t)(_ptr))
#define value_ptr(_val) ((void *)(uintptr_t)(_val))

typedef struct _proxy_random {
    uint64_t mask;
    uint64_t factor;
    uint64_t factor_inv;
    uint64_t shift;
} proxy_random_t;

static inline uint64_t
random_u64(void)
{
    uint64_t value;
    int32_t i;

    value = 0;
    for (i = 0; i < 4; i++) {
        value <<= 16;
        value ^= (random() >> 8) & 0xffff;
    }

    return value;
}

static inline void
random_init(proxy_random_t *rnd)
{
    uint64_t inv;

    rnd->mask = random_u64();

    do {
        rnd->factor = random_u64() | 1;
    } while (rnd->factor == 1);

    inv = rnd->factor & 0x3;
    inv *= 0x000000012 - rnd->factor * inv;
    inv *= 0x000000102 - rnd->factor * inv;
    inv *= 0x000010002 - rnd->factor * inv;
    inv *= 0x100000002 - rnd->factor * inv;
    rnd->factor_inv = inv * (2 - rnd->factor * inv);

    rnd->shift = random_u64();
}

static inline uint64_t
random_scramble(proxy_random_t *rnd, uint64_t value)
{
    uint32_t bits;

    bits = __builtin_popcountll(value);
    bits = ((rnd->shift >> bits) | (rnd->shift << (64 - bits))) & 0x3f;
    value = (value << bits) | (value >> (64 - bits));
    value ^= rnd->mask;

    return value * rnd->factor;
}

static inline uint64_t
random_unscramble(proxy_random_t *rnd, uint64_t value)
{
    uint32_t bits;

    value *= rnd->factor_inv;
    value ^= rnd->mask;
    bits = __builtin_popcountll(value);
    bits = ((rnd->shift >> bits) | (rnd->shift << (64 - bits))) & 0x3f;

    return (value >> bits) | (value << (64 - bits));
}

static inline void *
proxy_malloc(size_t size)
{
    void *ptr;

    ptr = malloc(size);
    if (ptr == NULL) {
        proxy_log(LOG_ERR, errno, "Failed to allocate memory");
    }

    return ptr;
}

static inline void
proxy_free(void *ptr)
{
    free(ptr);
}

static inline int32_t
proxy_mutex_init(pthread_mutex_t *mutex)
{
    int32_t err;

    err = pthread_mutex_init(mutex, NULL);
    if (err != 0) {
        return proxy_log(LOG_ERR, err, "Failed to initialize a mutex");
    }

    return 0;
}

static inline void
proxy_mutex_lock(pthread_mutex_t *mutex)
{
    int32_t err;

    err = pthread_mutex_lock(mutex);
    if (err != 0) {
        proxy_abort(err, "Mutex cannot be acquired");
    }
}

static inline void
proxy_mutex_unlock(pthread_mutex_t *mutex)
{
    int32_t err;

    err = pthread_mutex_unlock(mutex);
    if (err != 0) {
        proxy_abort(err, "Mutex cannot be released");
    }
}

static inline int32_t
proxy_rwmutex_init(pthread_rwlock_t *mutex)
{
    int32_t err;

    err = pthread_rwlock_init(mutex, NULL);
    if (err != 0) {
        return proxy_log(LOG_ERR, err, "Failed to initialize a rwmutex");
    }

    return 0;
}

static inline void
proxy_rwmutex_rdlock(pthread_rwlock_t *mutex)
{
    int32_t err;

    err = pthread_rwlock_rdlock(mutex);
    if (err != 0) {
        proxy_abort(err, "RWMutex cannot be acquired for read");
    }
}

static inline void
proxy_rwmutex_wrlock(pthread_rwlock_t *mutex)
{
    int32_t err;

    err = pthread_rwlock_wrlock(mutex);
    if (err != 0) {
        proxy_abort(err, "RWMutex cannot be acquired for write");
    }
}

static inline void
proxy_rwmutex_unlock(pthread_rwlock_t *mutex)
{
    int32_t err;

    err = pthread_rwlock_unlock(mutex);
    if (err != 0) {
        proxy_abort(err, "RWMutex cannot be released");
    }
}

static inline int32_t
proxy_condition_init(pthread_cond_t *condition)
{
    int32_t err;

    err = pthread_cond_init(condition, NULL);
    if (err != 0) {
        return proxy_log(LOG_ERR, err,
                         "Failed to initialize a condition variable");
    }

    return 0;
}

static inline void
proxy_condition_signal(pthread_cond_t *condition)
{
    int32_t err;

    err = pthread_cond_signal(condition);
    if (err != 0) {
        proxy_abort(err, "Condition variable cannot be signaled");
    }
}

static inline void
proxy_condition_wait(pthread_cond_t *condition, pthread_mutex_t *mutex)
{
    int32_t err;

    err = pthread_cond_wait(condition, mutex);
    if (err != 0) {
        proxy_abort(err, "Condition variable cannot be waited");
    }
}

static inline int32_t
proxy_thread_create(pthread_t *tid, void *(*main)(void *), void *arg)
{
    int32_t err;

    err = pthread_create(tid, NULL, main, arg);
    if (err != 0) {
        proxy_log(LOG_ERR, err, "Failed to create a thread");
    }

    return err;
}

static inline void
proxy_thread_kill(pthread_t tid, int32_t signum)
{
    int32_t err;

    err = pthread_kill(tid, signum);
    if (err != 0) {
        proxy_abort(err, "Failed to send a signal to a thread");
    }
}

static inline void
proxy_thread_join(pthread_t tid)
{
    int32_t err;

    err = pthread_join(tid, NULL);
    if (err != 0) {
        proxy_log(LOG_ERR, err, "Unable to join a thread");
    }
}

static inline int32_t
proxy_signal_set(int32_t signum, struct sigaction *action,
                 struct sigaction *old)
{
    if (sigaction(signum, action, old) < 0) {
        return proxy_log(LOG_ERR, errno, "Failed to configure a signal");
    }

    return 0;
}

#endif
