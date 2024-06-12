
#include "proxy_mount.h"
#include "proxy_helpers.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct _proxy_config {
    int32_t src;
    int32_t dst;
    int32_t size;
    int32_t total;
    void *buffer;
} proxy_config_t;

typedef struct _proxy_change {
    list_t list;
    uint32_t size;
    char data[];
} proxy_change_t;

typedef struct _proxy_iter {
    proxy_instance_t *instance;
    list_t *item;
} proxy_iter_t;

typedef struct _proxy_instance_pool {
    pthread_mutex_t mutex;
    list_t hash[256];
} proxy_mount_pool_t;

static proxy_mount_pool_t instance_pool = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
};

/* Ceph client instance sharing
 *
 * The main purpose of the libcephfs proxy is to avoid the multiple independent
 * data caches that are created when libcephfs is used from different processes.
 * However the cache is not created per process but per client instance, so each
 * call to `ceph_create()` creates its own private data cache instance. Just
 * forwarding the libcephfs API calls to a single proxy process is not enough to
 * solve the problem.
 *
 * The proxy will try to reuse existing client instances to reduce the number of
 * independent caches. However it's not always possible to map all proxy clients
 * to a single libcephfs instance. When different settings are used, separate
 * Ceph instances are required to avoid unwanted behaviours.
 *
 * Even though it's possible that some Ceph options may be compatible even if
 * they have different values, the proxy won't try to handle these cases. It
 * will consider the configuration as a black box, and only 100% equal
 * configurations will share the Ceph client instance.
 */

/* Ceph configuration file management
 *
 * We won't try to parse Ceph configuration files. The proxy only wants to know
 * if a configuration is equal or not. To do so, when a configuration file is
 * passed to the proxy, it will create a private copy and compute an SHA256
 * hash. If the hash doesn't match, the configuration is considered different,
 * even if it's not a real difference (like additional empty lines or the order
 * of the options).
 *
 * The private copy is necessary to enforce that the settings are not changed
 * concurrently, which could make us believe that two configurations are equal
 * when they are not.
 *
 * Besides a configuration file, the user can also make manual configuration
 * changes by using `ceph_conf_set()`. These changes are also tracked and
 * compared to be sure that the active configuration matches. Only if the
 * configuration file is exactly equal and all the applied changes are the same,
 * the Ceph client instance will be shared.
 */

static int32_t
proxy_config_source_prepare(const char *config, struct stat *st)
{
    int32_t fd, err;

    fd = open(config, O_RDONLY);
    if (fd < 0) {
        return proxy_log(LOG_ERR, errno, "open() failed");
    }

    if (fstat(fd, st) < 0) {
        err = proxy_log(LOG_ERR, errno, "fstat() failed");
        goto failed;
    }

    if (!S_ISREG(st->st_mode)) {
        err = proxy_log(LOG_ERR, EINVAL,
                        "Configuration file is not a regular file");
        goto failed;
    }

    return fd;

failed:
    close(fd);

    return err;
}

static void
proxy_config_source_close(int32_t fd)
{
    close(fd);
}

static int32_t
proxy_config_source_read(int32_t fd, void *buffer, size_t size)
{
    ssize_t len;

    len = read(fd, buffer, size);
    if (len < 0) {
        return proxy_log(LOG_ERR, errno, "read() failed");
    }

    return len;
}

static int32_t
proxy_config_source_validate(int32_t fd, struct stat *before, int32_t size)
{
    struct stat after;

    if (fstat(fd, &after) < 0) {
        return proxy_log(LOG_ERR, errno, "fstat() failed");
    }

    if ((before->st_size != size) ||
        (before->st_size != after.st_size) ||
        (before->st_blocks != after.st_blocks) ||
        (before->st_ctim.tv_sec != after.st_ctim.tv_sec) ||
        (before->st_ctim.tv_nsec != after.st_ctim.tv_nsec) ||
        (before->st_mtim.tv_sec != after.st_mtim.tv_sec) ||
        (before->st_mtim.tv_nsec != after.st_mtim.tv_nsec)) {
        proxy_log(LOG_WARN, 0, "Configuration file has been modified while "
                               "reading it");

        return 0;
    }

    return 1;
}

static int32_t
proxy_config_destination_prepare(void)
{
    int32_t fd;

    fd = openat(AT_FDCWD, ".", O_TMPFILE | O_WRONLY, 0600);
    if (fd < 0) {
        return proxy_log(LOG_ERR, errno, "openat() failed");
    }

    return fd;
}

static void
proxy_config_destination_close(int32_t fd)
{
    close(fd);
}

static int32_t
proxy_config_destination_write(int32_t fd, void *data, size_t size)
{
    ssize_t len;

    len = write(fd, data, size);
    if (len < 0) {
        return proxy_log(LOG_ERR, errno, "write() failed");
    }
    if (len != size) {
        return proxy_log(LOG_ERR, ENOSPC, "Partial write");
    }

    return size;
}

static int32_t
proxy_config_destination_commit(int32_t fd, const char *name)
{
    if (fsync(fd) < 0) {
        return proxy_log(LOG_ERR, errno, "fsync() failed");
    }

    if (linkat(fd, "", AT_FDCWD, name, AT_EMPTY_PATH) < 0) {
        if (errno != EEXIST) {
            return proxy_log(LOG_ERR, errno, "linkat() failed");
        }
    }

    return 0;
}

static int32_t
proxy_config_transfer(void **ptr, void *data, int32_t idx)
{
    proxy_config_t *cfg;
    int32_t len, err;

    cfg = data;

    len = proxy_config_source_read(cfg->src, cfg->buffer, cfg->size);
    if (len <= 0) {
        return len;
    }

    err = proxy_config_destination_write(cfg->dst, cfg->buffer, len);
    if (err < 0) {
        return err;
    }

    cfg->total += len;

    *ptr = cfg->buffer;

    return len;
}

static int32_t
proxy_config_prepare(const char *config, char *path, int32_t size)
{
    char hash[65];
    proxy_config_t cfg;
    struct stat before;
    int32_t err;

    cfg.size = 4096;
    cfg.buffer = proxy_malloc(cfg.size);
    if (cfg.buffer == NULL) {
        return -ENOMEM;
    }
    cfg.total = 0;

    cfg.src = proxy_config_source_prepare(config, &before);
    if (cfg.src < 0) {
        err = cfg.src;
        goto done_mem;
    }

    cfg.dst = proxy_config_destination_prepare();
    if (cfg.dst < 0) {
        err = cfg.dst;
        goto done_src;
    }

    err = proxy_hash_hex(hash, sizeof(hash), proxy_config_transfer, &cfg);
    if (err < 0) {
        goto done_dst;
    }

    err = proxy_config_source_validate(cfg.src, &before, cfg.total);
    if (err < 0) {
        goto done_dst;
    }

    err = snprintf(path, size, "ceph-%s.conf", hash);
    if (err < 0) {
        err = proxy_log(LOG_ERR, errno, "snprintf() failed");
        goto done_dst;
    }
    if (err >= size) {
        err = proxy_log(LOG_ERR, ENOBUFS,
                        "Insufficient space to store the name");
        goto done_dst;
    }

    err = proxy_config_destination_commit(cfg.dst, path);

done_dst:
    proxy_config_destination_close(cfg.dst);

done_src:
    proxy_config_source_close(cfg.src);

done_mem:
    proxy_free(cfg.buffer);

    return err;
}

static int32_t
proxy_instance_change_add(proxy_instance_t *instance, const char *arg1,
                          const char *arg2, const char *arg3)
{
    proxy_change_t *change;
    int32_t len[3], total;

    len[0] = strlen(arg1) + 1;
    len[1] = strlen(arg2) + 1;
    len[2] = 0;
    if (arg3 != NULL) {
        len[2] = strlen(arg3) + 1;
    }

    total = len[0] + len[1] + len[2];

    change = proxy_malloc(sizeof(proxy_change_t) + total);
    if (change == NULL) {
        return -ENOMEM;
    }
    change->size = total;

    memcpy(change->data, arg1, len[0]);
    memcpy(change->data + len[0], arg2, len[1]);
    if (arg3 != NULL) {
        memcpy(change->data + len[0] + len[1], arg3, len[2]);
    }

    list_add_tail(&change->list, &instance->changes);

    return 0;
}

static void
proxy_instance_change_del(proxy_instance_t *instance)
{
    proxy_change_t *change;

    change = list_last_entry(&instance->changes, proxy_change_t, list);
    list_del(&change->list);

    proxy_free(change);
}

/* Destroy a Ceph client instance */
static void
proxy_instance_destroy(proxy_instance_t *instance)
{
    if (instance->mounted) {
        ceph_unmount(instance->cmount);
    }

    if (instance->cmount != NULL) {
        ceph_release(instance->cmount);
    }

    while (!list_empty(&instance->changes)) {
        proxy_instance_change_del(instance);
    }

    proxy_free(instance);
}

/* Create a new Ceph client instance with the provided id */
static int32_t
proxy_instance_create(proxy_instance_t **pinstance, const char *id)
{
    struct ceph_mount_info *cmount;
    proxy_instance_t *instance;
    int32_t err;

    instance = proxy_malloc(sizeof(proxy_instance_t));
    if (instance == NULL) {
        return -ENOMEM;
    }

    list_init(&instance->siblings);
    list_init(&instance->changes);
    instance->cmount = NULL;
    instance->inited = false;
    instance->mounted = false;

    err = proxy_instance_change_add(instance, "id", id, NULL);
    if (err < 0) {
        goto failed;
    }

    err = ceph_create(&cmount, id);
    if (err < 0) {
        proxy_log(LOG_ERR, -err, "ceph_create() failed");
        goto failed;
    }

    instance->cmount = cmount;

    *pinstance = instance;

    return 0;

failed:
    proxy_instance_destroy(instance);

    return err;
}

static int32_t
proxy_instance_release(proxy_instance_t *instance)
{
    if (instance->mounted) {
        return proxy_log(LOG_ERR, EISCONN,
                         "Cannot release an active connection");
    }

    proxy_instance_destroy(instance);

    return 0;
}

/* Assign a configuration file to the instance. */
static int32_t
proxy_instance_config(proxy_instance_t *instance, const char *config)
{
    char path[128];
    int32_t err;

    if (instance->mounted) {
        return proxy_log(LOG_ERR, EISCONN,
                         "Cannot configure a mounted instance");
    }

    err = proxy_config_prepare(config, path, sizeof(path));
    if (err < 0) {
        return err;
    }

    err = proxy_instance_change_add(instance, "conf", path, NULL);
    if (err < 0) {
        return err;
    }

    err = ceph_conf_read_file(instance->cmount, path);
    if (err < 0) {
        proxy_instance_change_del(instance);
    }

    return err;
}

static int32_t
proxy_instance_option_get(proxy_instance_t *instance, const char *name,
                          char *value, size_t size)
{
    int32_t err, res;

    res = ceph_conf_get(instance->cmount, name, value, size);
    if (res < 0) {
        return proxy_log(LOG_ERR, -res,
                         "Failed to get configuration from a client instance");
    }

    err = proxy_instance_change_add(instance, "get", name, value);
    if (err < 0) {
        return err;
    }

    return res;
}

static int32_t
proxy_instance_option_set(proxy_instance_t *instance, const char *name,
                          const char *value)
{
    int32_t err;

    if (instance->mounted) {
        return proxy_log(LOG_ERR, EISCONN,
                         "Cannot configure a mounted instance");
    }

    err = proxy_instance_change_add(instance, "set", name, value);
    if (err < 0) {
        return err;
    }

    err = ceph_conf_set(instance->cmount, name, value);
    if (err < 0) {
        proxy_log(LOG_ERR, -err, "Failed to configure a client instance");
        proxy_instance_change_del(instance);
    }

    return err;
}

static int32_t
proxy_instance_select(proxy_instance_t *instance, const char *fs)
{
    int32_t err;

    if (instance->mounted) {
        return proxy_log(LOG_ERR, EISCONN,
                         "Cannot select a filesystem on a mounted instance");
    }

    err = proxy_instance_change_add(instance, "fs", fs, NULL);
    if (err < 0) {
        return err;
    }

    err = ceph_select_filesystem(instance->cmount, fs);
    if (err < 0) {
        proxy_log(LOG_ERR, -err,
                  "Failed to select a filesystem on a client instance");
        proxy_instance_change_del(instance);
    }

    return err;
}

static int32_t
proxy_instance_init(proxy_instance_t *instance)
{
    int32_t err;

    if (instance->mounted || instance->inited) {
        return 0;
    }

    err = ceph_init(instance->cmount);
    if (err < 0) {
        return proxy_log(LOG_ERR, -err, "ceph_init() failed");
    }

    instance->inited = true;

    return 0;
}

static int32_t
proxy_instance_hash(void **ptr, void *data, int32_t idx)
{
    proxy_iter_t *iter;
    proxy_change_t *change;

    iter = data;

    if (iter->item == &iter->instance->changes) {
        return 0;
    }

    change = list_entry(iter->item, proxy_change_t, list);
    iter->item = iter->item->next;

    *ptr = change->data;

    return change->size;
}

static int32_t
proxy_instance_mount(proxy_instance_t **pinstance)
{
    proxy_instance_t *instance, *existing;
    proxy_iter_t iter;
    list_t *list;
    int32_t err;

    instance = *pinstance;

    if (instance->mounted) {
        return proxy_log(LOG_ERR, EISCONN,
                         "Cannot mount and already mounted instance");
    }

    iter.instance = instance;
    iter.item = instance->changes.next;

    err = proxy_hash(instance->hash, sizeof(instance->hash),
                     proxy_instance_hash, &iter);
    if (err < 0) {
        return err;
    }

    list = &instance_pool.hash[instance->hash[0]];

    proxy_mutex_lock(&instance_pool.mutex);

    if (list->next == NULL) {
        list_init(list);
    } else {
        list_for_each_entry(existing, list, list) {
            if (memcmp(existing->hash, instance->hash, 32) == 0) {
                list_add(&instance->list, &existing->siblings);
                goto found;
            }
        }
    }

    err = ceph_mount(instance->cmount, "/");
    if (err >= 0) {
        instance->inited = true;
        instance->mounted = true;
        list_add(&instance->list, list);
    }

    existing = NULL;

found:
    proxy_mutex_unlock(&instance_pool.mutex);

    if (err < 0) {
        return proxy_log(LOG_ERR, -err, "ceph_mount() failed");
    }

    if (existing != NULL) {
        proxy_log(LOG_INFO, 0, "Shared a client instance (%p)", existing);
        *pinstance = existing;
    } else {
        proxy_log(LOG_INFO, 0, "Created a new client instance (%p)", instance);
    }

    return 0;
}

static int32_t
proxy_instance_unmount(proxy_instance_t **pinstance)
{
    proxy_instance_t *instance, *sibling;
    int32_t err;

    instance = *pinstance;

    if (!instance->mounted) {
        return proxy_log(LOG_ERR, ENOTCONN,
                         "Cannot unmount an already unmount instance");
    }

    sibling = NULL;

    proxy_mutex_lock(&instance_pool.mutex);

    if (list_empty(&instance->siblings)) {
        list_del(&instance->list);
        instance->mounted = false;
    } else {
        sibling = list_first_entry(&instance->siblings, proxy_instance_t, list);
        list_del_init(&sibling->list);
    }

    proxy_mutex_unlock(&instance_pool.mutex);

    if (sibling == NULL) {
        err = ceph_unmount(instance->cmount);
        if (err < 0) {
            return proxy_log(LOG_ERR, -err, "ceph_unmount() failed");
        }
    } else {
        *pinstance = sibling;
    }

    return 0;
}

int32_t
proxy_mount_create(proxy_mount_t **pmount, const char *id)
{
    proxy_mount_t *mount;
    int32_t err;

    mount = proxy_malloc(sizeof(proxy_mount_t));
    if (mount == NULL) {
        return -ENOMEM;
    }
    mount->root = NULL;

    err = proxy_instance_create(&mount->instance, id);
    if (err < 0) {
        proxy_free(mount);
        return err;
    }

    *pmount = mount;

    return 0;
}

int32_t
proxy_mount_config(proxy_mount_t *mount, const char *config)
{
    return proxy_instance_config(mount->instance, config);
}

int32_t
proxy_mount_set(proxy_mount_t *mount, const char *name, const char *value)
{
    return proxy_instance_option_set(mount->instance, name, value);
}

int32_t
proxy_mount_get(proxy_mount_t *mount, const char *name, char *value,
                size_t size)
{
    return proxy_instance_option_get(mount->instance, name, value, size);
}

int32_t
proxy_mount_select(proxy_mount_t *mount, const char *fs)
{
    return proxy_instance_select(mount->instance, fs);
}

int32_t
proxy_mount_init(proxy_mount_t *mount)
{
    return proxy_instance_init(mount->instance);
}

int32_t
proxy_mount_mount(proxy_mount_t *mount, const char *root)
{
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct UserPerm *userperms;
    struct Inode *inode;
    int32_t err;

    err = proxy_instance_mount(&mount->instance);
    if (err < 0) {
        return err;
    }

    cmount = proxy_cmount(mount);

    userperms = ceph_mount_perms(cmount);

    if (root == NULL) {
        root = "/";
    }

    err = ceph_ll_walk(cmount, root, &inode, &stx, CEPH_STATX_ALL_STATS, 0,
                       userperms);
    if (err < 0) {
        proxy_log(LOG_ERR, -err, "ceph_ll_walk() failed");
        proxy_instance_unmount(&mount->instance);
        return err;
    }

    mount->root = inode;

    return 0;
}

int32_t
proxy_mount_unmount(proxy_mount_t *mount)
{
    if (mount->root != NULL) {
        ceph_ll_forget(mount->instance->cmount, mount->root, 1);
        mount->root = NULL;
    }

    return proxy_instance_unmount(&mount->instance);
}

int32_t
proxy_mount_release(proxy_mount_t *mount)
{
    int32_t err;

    err = proxy_instance_release(mount->instance);
    if (err >= 0) {
        proxy_free(mount);
    }

    return err;
}
