
#ifndef __LIBCEPHFSD_PROXY_MOUNT_H__
#define __LIBCEPHFSD_PROXY_MOUNT_H__

#include "proxy.h"
#include "proxy_list.h"

#include <cephfs/libcephfs.h>

typedef struct _proxy_instance {
    uint8_t hash[32];
    list_t list;
    list_t siblings;
    list_t changes;
    struct ceph_mount_info *cmount;
    bool inited;
    bool mounted;
} proxy_instance_t;

typedef struct _proxy_mount {
    proxy_instance_t *instance;
    struct Inode *root;
    uint64_t root_ino;
} proxy_mount_t;

static inline struct ceph_mount_info *
proxy_cmount(proxy_mount_t *mount)
{
    return mount->instance->cmount;
}

int32_t
proxy_mount_create(proxy_mount_t **pmount, const char *id);

int32_t
proxy_mount_config(proxy_mount_t *mount, const char *config);

int32_t
proxy_mount_set(proxy_mount_t *mount, const char *name, const char *value);

int32_t
proxy_mount_get(proxy_mount_t *mount, const char *name, char *value,
                size_t size);

int32_t
proxy_mount_select(proxy_mount_t *mount, const char *fs);

int32_t
proxy_mount_init(proxy_mount_t *mount);

int32_t
proxy_mount_mount(proxy_mount_t *mount, const char *root);

int32_t
proxy_mount_unmount(proxy_mount_t *mount);

int32_t
proxy_mount_release(proxy_mount_t *mount);

#endif
