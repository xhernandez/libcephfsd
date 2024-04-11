
#include <stdio.h>
#include <unistd.h>
#include <endian.h>
#include <ctype.h>

#include <cephfs/libcephfs.h>

#include "proxy_manager.h"
#include "proxy_link.h"
#include "proxy_buffer.h"
#include "proxy_helpers.h"
#include "proxy_log.h"
#include "proxy_requests.h"

typedef struct _proxy_server {
    proxy_link_t link;
    proxy_manager_t *manager;
} proxy_server_t;

typedef struct _proxy_client {
    proxy_worker_t worker;
    proxy_buffer_t buffer_read;
    proxy_buffer_t buffer_write;
    proxy_log_handler_t log_handler;
    proxy_link_t *link;
    pthread_mutex_t log_mutex;
    proxy_random_t random;
    int32_t sd;
} proxy_client_t;

typedef struct _proxy {
    proxy_manager_t manager;
    proxy_log_handler_t log_handler;
    const char *socket_path;
} proxy_t;

typedef struct _client_command {
    const char *name;
    void (*handler)(proxy_client_t *);
} client_command_t;

typedef int32_t (*proxy_handler_t)(proxy_client_t *, proxy_req_t *,
                                   const void *data, int32_t data_size);

static __thread char recv_buffer[1048576 * 4];

static proxy_random_t global_random;

/*
struct _proxy_link_cmd {
    uint16_t op;
    uint16_t len;
    uint32_t id;
};

struct _proxy_link_op {
    int32_t (*handler)(proxy_link_buffer_t *, void *);
    uint32_t req_size;
    uint32_t ans_size;
};

typedef proxy_link_cmd_t libcephfsd_null_req_t;
typedef proxy_link_cmd_t libcephfsd_null_ans_t;

typedef proxy_link_cmd_t libcephfsd_version_req_t;
typedef struct _libcephfsd_version_ans {
    proxy_link_cmd_t cmd;
    uint32_t major;
    uint32_t minor;
    uint32_t release;
    proxy_string_t text;
} libcephfsd_version_ans_t;

static proxy_link_worker_t *logging_worker = NULL;

static int32_t
proxy_client_text(proxy_link_buffer_t *buffer)
{
    char *cmd;
    int32_t err;

    err = proxy_link_write(buffer->worker, "libcephfsd 0.1\n", 15);
    if (err < 0) {
        return err;
    }

    do {
        err = proxy_link_buffer_read_line(buffer);
        if (err < 0) {
            return err;
        }

        cmd = proxy_link_buffer_ptr(buffer, err + 1);
        while ((err > 0) && isspace(cmd[err - 1])) {
            err--;
        }
        cmd[err] = 0;
        while (isspace(*cmd)) {
            cmd++;
        }
        if (*cmd == 0) {
            continue;
        }

        if (strcmp(cmd, "quit") == 0) {
            err = proxy_link_write(buffer->worker, "Shutting down\n", 14);
            proxy_link_shutdown(buffer->worker->manager);
            break;
        }

        printf("cmd: '%s'\n", cmd);
        err = proxy_link_write(buffer->worker, "Unknown command\n", 16);
    } while (err >= 0);

    return err;
}

static int32_t
proxy_client_binary(proxy_link_buffer_t *buffer)
{
    proxy_link_cmd_t *cmd;
    uint16_t version[2];
    int32_t err;

    version[0] = 0;
    version[1] = 1;
    err = proxy_link_write(buffer->worker, version, sizeof(version));
    if (err < 0) {
        return err;
    }

    do {
        err = proxy_link_buffer_read(buffer, sizeof(proxy_link_cmd_t));
        if (err < 0) {
            return err;
        }

        cmd = proxy_link_buffer_ptr(buffer, sizeof(proxy_link_cmd_t));
        if (cmd->op >= LIBCEPHFSD_OP_TOTAL_OPS) {
            err = proxy_log("Operation not supported", ENOSYS);
        } else if (libcephfsd_ops[cmd->op] == NULL) {
            err = proxy_log("Operation not implemented", EOPNOTSUPP);
        } else {
            err = libcephfsd_ops[cmd->op](buffer, cmd);
        }
    } while (err >= 0);

    return err;
}
*/

static int32_t
client_buffer_read(proxy_buffer_t *buffer, void *ptr, int32_t size)
{
    proxy_client_t *client;

    client = container_of(buffer, proxy_client_t, buffer_read);

    return proxy_link_read(client->link, client->sd, ptr, size);
}

static proxy_buffer_ops_t client_read_ops = {
    .read = client_buffer_read
};

static int32_t
client_buffer_write(proxy_buffer_t *buffer, void *ptr, int32_t size)
{
    proxy_client_t *client;

    client = container_of(buffer, proxy_client_t, buffer_write);

    return proxy_link_write(client->link, client->sd, ptr, size);
}

static proxy_buffer_ops_t client_write_ops = {
    .write = client_buffer_write
};

static int32_t
client_init(proxy_client_t *client, int32_t size)
{
    int32_t err;

    err = proxy_buffer_open(&client->buffer_read, &client_read_ops, NULL, size,
                            BUFFER_READ);
    if (err < 0) {
        return err;
    }

    err = proxy_buffer_open(&client->buffer_write, &client_write_ops, NULL,
                            size, BUFFER_WRITE);
    if (err < 0) {
        goto failed_buffer_read;
    }

    err = proxy_mutex_init(&client->log_mutex);
    if (err < 0) {
        goto failed_buffer_write;
    }

    return 0;

failed_buffer_write:
    proxy_buffer_close(&client->buffer_write);

failed_buffer_read:
    proxy_buffer_close(&client->buffer_read);

    return err;
}

static void
client_destroy(proxy_client_t *client)
{
    pthread_mutex_destroy(&client->log_mutex);

    proxy_buffer_close(&client->buffer_write);
    proxy_buffer_close(&client->buffer_read);
}

static int32_t
client_write_args(proxy_client_t *client, const char *fmt, va_list args)
{
    int32_t err;

    proxy_mutex_lock(&client->log_mutex);

    err = proxy_buffer_write_format_args(&client->buffer_write, fmt, args);
    if (err >= 0) {
        err = proxy_buffer_flush(&client->buffer_write);
    }

    proxy_mutex_unlock(&client->log_mutex);

    return err;
}

static int32_t
client_write(proxy_client_t *client, const char *fmt, ...)
{
    va_list args;
    int32_t err;

    va_start(args, fmt);
    err = client_write_args(client, fmt, args);
    va_end(args);

    return err;
}

static void
client_log_handler(proxy_log_handler_t *handler, int32_t level, int32_t err,
                   const char *msg)
{
    proxy_client_t *client;

    client = container_of(handler, proxy_client_t, log_handler);

    client_write(client, "[%d] %s\n", level, msg);
}

static int32_t
send_error(proxy_client_t *client, int32_t error)
{
    proxy_link_ans_t ans;
    struct iovec iov[1];

    iov[0].iov_base = &ans;
    iov[0].iov_len = sizeof(ans);

    return proxy_link_ans_send(client->sd, error, iov, 1);
}

static uint64_t
uint64_checksum(uint64_t value)
{
    value = (value & 0xff00ff00ff00ffULL) +
            ((value >> 8) & 0xff00ff00ff00ffULL);
    value += value >> 16;
    value += value >> 32;

    return value & 0xff;
}

static int32_t
ptr_checksum(proxy_random_t *rnd, void *ptr, uint64_t *pvalue)
{
    uint64_t value;

    if (ptr == NULL) {
        *pvalue = 0;
        return 0;
    }

    value = (uint64_t)(uintptr_t)ptr;
    if ((value & 0xff00000000000007ULL) != 0) {
        proxy_log(LOG_ERR, EIO, "Unexpected pointer value");
        return -EIO;
    }

    value -= uint64_checksum(value) << 56;

    *pvalue = random_scramble(rnd, value);

    return 0;
}

static int32_t
ptr_check(proxy_random_t *rnd, uint64_t value, void **pptr)
{
    if (value == 0) {
        *pptr = NULL;
        return 0;
    }

    value = random_unscramble(rnd, value);

    if ((uint64_checksum(value) != 0) || ((value & 7) != 0)) {
        proxy_log(LOG_ERR, EFAULT, "Unexpected pointer value");
        return -EFAULT;
    }

    *pptr = (void *)(uintptr_t)(value & 0xffffffffffffffULL);

    return 0;
}

#define CEPH_COMPLETE(_client, _err, _ans) \
    ({ \
        int32_t __err = (_err); \
        if (__err < 0) { \
            __err = send_error(_client, __err); \
        } else { \
            __err = CEPH_RET(_client->sd, __err, _ans); \
        } \
        __err; \
    })

#if 0
#define TRACE(_fmt, _args...) do { } while (0)
#else
#define TRACE(_fmt, _args...) printf(_fmt "\n", ## _args)
#endif

static int32_t
libcephfsd_version(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_version, ans, 1);
    const char *text;
    int32_t major, minor, patch;

    text = ceph_version(&major, &minor, &patch);
    TRACE("ceph_version(%d, %d, %d) -> %s", major, minor, patch, text);

    ans.major = major;
    ans.minor = minor;
    ans.patch = patch;

    CEPH_STR_ADD(ans, text, text);

    return CEPH_RET(client->sd, 0, ans);
}

static int32_t
libcephfsd_userperm_new(proxy_client_t *client, proxy_req_t *req,
                        const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_userperm_new, ans, 0);
    UserPerm *userperm;
    int32_t err;

    userperm = ceph_userperm_new(req->userperm_new.uid, req->userperm_new.gid,
                                 req->userperm_new.groups, (gid_t *)data);
    TRACE("ceph_userperm_new(%u, %u, %u) -> %p", req->userperm_new.uid,
          req->userperm_new.gid, req->userperm_new.groups, userperm);

    if (userperm == NULL) {
        err = -ENOMEM;
    } else {
        err = ptr_checksum(&global_random, userperm, &ans.userperm);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_userperm_destroy(proxy_client_t *client, proxy_req_t *req,
                            const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_userperm_destroy, ans, 0);
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&global_random, req->userperm_destroy.userperm,
                    (void **)&perms);
    TRACE("ceph_userperm_destroy(%p) -> %d", perms, err);

    if (err >= 0) {
        ceph_userperm_destroy(perms);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_create(proxy_client_t *client, proxy_req_t *req, const void *data,
                  int32_t data_size)
{
    CEPH_DATA(ceph_create, ans, 0);
    struct ceph_mount_info *cmount;
    const char *id;
    int32_t err;

    id = CEPH_STR_GET(req->create, id, data);

    err = ceph_create(&cmount, id);
    TRACE("ceph_create(%p, '%s') -> %d", cmount, id, err);

    if (err >= 0) {
        err = ptr_checksum(&client->random, cmount, &ans.cmount);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_release(proxy_client_t *client, proxy_req_t *req, const void *data,
                  int32_t data_size)
{
    CEPH_DATA(ceph_release, ans, 0);
    struct ceph_mount_info *cmount;
    int32_t err;

    err = ptr_check(&client->random, req->release.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ceph_release(cmount);
        TRACE("ceph_release(%p) -> %d", cmount, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_conf_read_file(proxy_client_t *client, proxy_req_t *req,
                          const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_conf_read_file, ans, 0);
    struct ceph_mount_info *cmount;
    const char *path;
    int32_t err;

    err = ptr_check(&client->random, req->conf_read_file.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        path = CEPH_STR_GET(req->conf_read_file, path, data);

        err = ceph_conf_read_file(cmount, path);
        TRACE("ceph_conf_read_file(%p, '%s') ->%d", cmount, path, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_conf_get(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_conf_get, ans, 1);
    struct ceph_mount_info *cmount;
    const char *option;
    int32_t err;

    err = 0;
    if (req->conf_get.size > sizeof(recv_buffer)) {
        err = proxy_log(LOG_ERR, EINVAL, "Option buffer too large");
    }
    if (err >= 0) {
        err = ptr_check(&client->random, req->conf_get.cmount,
                        (void **)&cmount);
    }
    if (err >= 0) {
        option = CEPH_STR_GET(req->conf_get, option, data);

        err = ceph_conf_get(cmount, option, recv_buffer, req->conf_get.size);
        TRACE("ceph_conf_get(%p, '%s', '%s') -> %d", cmount, option,
               recv_buffer, err);

        if (err >= 0) {
            CEPH_DATA_ADD(ans, value, recv_buffer, strlen(recv_buffer) + 1);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_conf_set(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_conf_set, ans, 0);
    struct ceph_mount_info *cmount;
    const char *option, *value;
    int32_t err;

    err = ptr_check(&client->random, req->conf_set.cmount, (void **)&cmount);
    if (err >= 0) {
        option = CEPH_STR_GET(req->conf_set, option, data);
        value = CEPH_STR_GET(req->conf_set, value, data + req->conf_set.option);

        err = ceph_conf_set(cmount, option, value);
        TRACE("ceph_conf_set(%p, '%s', '%s') -> %d", cmount, option, value,
               err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_init(proxy_client_t *client, proxy_req_t *req, const void *data,
                int32_t data_size)
{
    CEPH_DATA(ceph_init, ans, 0);
    struct ceph_mount_info *cmount;
    int32_t err;

    err = ptr_check(&client->random, req->init.cmount, (void **)&cmount);

    if (err >= 0) {
        err = ceph_init(cmount);
        TRACE("ceph_init(%p) -> %d", cmount, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_select_filesystem(proxy_client_t *client, proxy_req_t *req,
                             const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_select_filesystem, ans, 0);
    struct ceph_mount_info *cmount;
    const char *fs;
    int32_t err;

    err = ptr_check(&client->random, req->select_filesystem.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        fs = CEPH_STR_GET(req->select_filesystem, fs, data);

        err = ceph_select_filesystem(cmount, fs);
        TRACE("ceph_select_filesystem(%p, '%s') -> %d", cmount, fs, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_mount(proxy_client_t *client, proxy_req_t *req, const void *data,
                 int32_t data_size)
{
    CEPH_DATA(ceph_mount, ans, 0);
    struct ceph_mount_info *cmount;
    const char *root;
    int32_t err;

    err = ptr_check(&client->random, req->mount.cmount, (void **)&cmount);
    if (err >= 0) {
        root = CEPH_STR_GET(req->mount, root, data);

        err = ceph_mount(cmount, root);
        TRACE("ceph_mount(%p, '%s') -> %d", cmount, root, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_unmount(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_unmount, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *root_inode, *cwd_inode;
    int32_t err;

    err = ptr_check(&client->random, req->unmount.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->unmount.root_inode,
                        (void **)&root_inode);
    }
    if (err >= 0) {
        err = ptr_check(&client->random, req->unmount.cwd_inode,
                        (void **)&cwd_inode);
    }

    if (err >= 0) {
        if (root_inode != NULL) {
            err = ceph_ll_put(cmount, root_inode);
        }
        if ((err >= 0) && (cwd_inode != NULL)) {
            err = ceph_ll_put(cmount, cwd_inode);
        }
        if (err >= 0) {
            err = ceph_unmount(cmount);
        }
        TRACE("ceph_unmount(%p) -> %d", cmount, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_statfs(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_ll_statfs, ans, 1);
    struct statvfs st;
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    int32_t err;

    err = ptr_check(&client->random, req->ll_statfs.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_statfs.inode, (void **)&inode);
    }

    if (err >= 0) {
        CEPH_BUFF_ADD(ans, &st, sizeof(st));

        err = ceph_ll_statfs(cmount, inode, &st);
        TRACE("ceph_ll_statfs(%p, %p) -> %d", cmount, inode, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_lookup(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_ll_lookup, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *parent, *out;
    const char *name;
    UserPerm *perms;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_lookup.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_lookup.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_lookup.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        want = req->ll_lookup.want;
        flags = req->ll_lookup.flags;
        name = CEPH_STR_GET(req->ll_lookup, name, data);

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_lookup(cmount, parent, name, &out, &stx, want, flags,
                             perms);
        TRACE("ceph_ll_lookup(%p, %p, '%s', %p, %x, %x, %p) -> %d", cmount,
              parent, name, out, want, flags, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, out, &ans.inode);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_lookup_inode(proxy_client_t *client, proxy_req_t *req,
                           const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_lookup_inode, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    struct inodeno_t ino;
    int32_t err;

    err = ptr_check(&client->random, req->ll_lookup_inode.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        ino = req->ll_lookup_inode.ino;

        err = ceph_ll_lookup_inode(cmount, ino, &inode);
        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }

        TRACE("ceph_ll_lookup_inode(%p, %lu, %p) -> %d", cmount, ino.val, inode,
              err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_lookup_root(proxy_client_t *client, proxy_req_t *req,
                          const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_lookup_root, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    int32_t err;

    err = ptr_check(&client->random, req->ll_lookup_root.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        err = ceph_ll_lookup_root(cmount, &inode);
        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }

        TRACE("ceph_ll_lookup_root(%p, %p) -> %d", cmount, inode, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_put(proxy_client_t *client, proxy_req_t *req, const void *data,
                  int32_t data_size)
{
    CEPH_DATA(ceph_ll_put, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    int32_t err;

    err = ptr_check(&client->random, req->ll_put.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_put.inode, (void **)&inode);
    }

    if (err >= 0) {
        err = ceph_ll_put(cmount, inode);
        TRACE("ceph_ll_put(%p, %p) -> %d", cmount, inode, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_walk(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_ll_walk, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    const char *path;
    UserPerm *perms;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_walk.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_walk.userperm, (void **)&perms);
    }
    if (err >= 0) {
        want = req->ll_walk.want;
        flags = req->ll_walk.flags;
        path = CEPH_STR_GET(req->ll_walk, path, data);

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_walk(cmount, path, &inode, &stx, want, flags, perms);
        TRACE("ceph_ll_walk(%p, '%s', %p, %x, %x, %p) -> %d", cmount, path,
              inode, want, flags, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_chdir(proxy_client_t *client, proxy_req_t *req, const void *data,
                 int32_t data_size)
{
    CEPH_DATA(ceph_chdir, ans, 1);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    const char *path;
    int32_t err;

    err = ptr_check(&client->random, req->chdir.cmount, (void **)&cmount);
    if (err >= 0) {
        path = CEPH_STR_GET(req->chdir, path, data);

        err = ceph_chdir(cmount, path);
        if (err >= 0) {
            if (req->chdir.inode != 0) {
                err = ptr_check(&client->random, req->chdir.inode,
                                (void **)&inode);
                if (err >= 0) {
                    ceph_ll_put(cmount, inode);
                }
            }

            path = ceph_getcwd(cmount);
            CEPH_BUFF_ADD(ans, path, strlen(path) + 1);
        }
        TRACE("ceph_chdir(%p, '%s') -> %d", cmount, path, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_getcwd(proxy_client_t *client, proxy_req_t *req, const void *data,
                  int32_t data_size)
{
    CEPH_DATA(ceph_getcwd, ans, 1);
    struct ceph_mount_info *cmount;
    const char *path;
    int32_t err;

    err = ptr_check(&client->random, req->getcwd.cmount, (void **)&cmount);

    if (err >= 0) {
        path = ceph_getcwd(cmount);
        TRACE("ceph_getcwd(%p) -> '%s'", cmount, path);

        if (path == NULL) {
            err = -errno;
        } else {
            CEPH_STR_ADD(ans, path, path);
            err = 0;
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_readdir(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_readdir, ans, 1);
    struct ceph_mount_info *cmount;
    struct ceph_dir_result *dirp;
    struct dirent *de;
    int32_t err;

    err = ptr_check(&client->random, req->readdir.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->readdir.dir, (void **)&dirp);
    }

    if (err >= 0) {
        de = ceph_readdir(cmount, dirp);
        TRACE("ceph_readdir(%p, %p) -> %p", cmount, dirp, de);
        if (de == NULL) {
            err = -errno;
        } else {
            CEPH_BUFF_ADD(ans, de,
                          offset_of(struct dirent, d_name) +
                              strlen(de->d_name) + 1);
            err = 0;
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_rewinddir(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_rewinddir, ans, 0);
    struct ceph_mount_info *cmount;
    struct ceph_dir_result *dirp;
    int32_t err;

    err = ptr_check(&client->random, req->rewinddir.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->rewinddir.dir, (void **)&dirp);
    }

    if (err >= 0) {
        ceph_rewinddir(cmount, dirp);
        TRACE("ceph_rewinddir(%p, %p)", cmount, dirp);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_open(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_ll_open, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    UserPerm *perms;
    struct Fh *fh;
    int32_t flags, err;

    err = ptr_check(&client->random, req->ll_open.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_open.inode, (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_open.userperm, (void **)&perms);
    }
    if (err >= 0) {
        flags = req->ll_open.flags;

        err = ceph_ll_open(cmount, inode, flags, &fh, perms);
        TRACE("ceph_ll_open(%p, %p, %x, %p, %p) -> %d", cmount, inode, flags,
              fh, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, fh, &ans.fh);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_create(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_ll_create, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *parent, *inode;
    struct Fh *fh;
    const char *name;
    UserPerm *perms;
    mode_t mode;
    uint32_t want, flags;
    int32_t oflags, err;

    err = ptr_check(&client->random, req->ll_create.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_create.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_create.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        mode = req->ll_create.mode;
        oflags = req->ll_create.oflags;
        want = req->ll_create.want;
        flags = req->ll_create.flags;
        name = CEPH_STR_GET(req->ll_create, name, data);

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_create(cmount, parent, name, mode, oflags, &inode, &fh,
                             &stx, want, flags, perms);
        TRACE("ceph_ll_create(%p, %p, '%s', %o, %x, %p, %p, %x, %x, %p) -> %d",
              cmount, parent, name, mode, oflags, inode, fh, want, flags,
              perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, fh, &ans.fh);
            if (err >= 0) {
                err = ptr_checksum(&client->random, inode, &ans.inode);
            }
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_mknod(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_mknod, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *parent, *inode;
    const char *name;
    UserPerm *perms;
    dev_t rdev;
    mode_t mode;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_mknod.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_mknod.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_mknod.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        mode = req->ll_mknod.mode;
        rdev = req->ll_mknod.rdev;
        want = req->ll_mknod.want;
        flags = req->ll_mknod.flags;
        name = CEPH_STR_GET(req->ll_mknod, name, data);

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_mknod(cmount, parent, name, mode, rdev, &inode, &stx, want,
                            flags, perms);
        TRACE("ceph_ll_mknod(%p, %p, '%s', %o, %lx, %p, %x, %x, %p) -> %d",
              cmount, parent, name, mode, rdev, inode, want, flags, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_close(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_close, ans, 0);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    int32_t err;

    err = ptr_check(&client->random, req->ll_close.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_close.fh, (void **)&fh);
    }

    if (err >= 0) {
        err = ceph_ll_close(cmount, fh);
        TRACE("ceph_ll_close(%p, %p) -> %d", cmount, fh, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_rename(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_ll_rename, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *old_parent, *new_parent;
    const char *old_name, *new_name;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_rename.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_rename.old_parent,
                        (void **)&old_parent);
    }
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_rename.new_parent,
                        (void **)&new_parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_rename.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        old_name = CEPH_STR_GET(req->ll_rename, old_name, data);
        new_name = CEPH_STR_GET(req->ll_rename, new_name,
                                data + req->ll_rename.old_name);

        err = ceph_ll_rename(cmount, old_parent, old_name, new_parent, new_name,
                            perms);
        TRACE("ceph_ll_rename(%p, %p, '%s', %p, '%s', %p) -> %d", cmount,
              old_parent, old_name, new_parent, new_name, perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_lseek(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_lseek, ans, 0);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    off_t offset, pos;
    int32_t whence, err;

    err = ptr_check(&client->random, req->ll_lseek.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_lseek.fh, (void **)&fh);
    }
    if (err >= 0) {
        offset = req->ll_lseek.offset;
        whence = req->ll_lseek.whence;

        pos = ceph_ll_lseek(cmount, fh, offset, whence);
        TRACE("ceph_ll_lseek(%p, %p, %ld, %d) -> %ld", cmount, fh, offset,
              whence, pos);

        if (pos < 0) {
            err = -errno;
        } else {
            ans.offset = pos;
            err = 0;
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_read(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_ll_read, ans, 1);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    uint64_t len;
    int64_t offset;
    int32_t err;

    err = ptr_check(&client->random, req->ll_read.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_read.fh, (void **)&fh);
    }
    if (err >= 0) {
        offset = req->ll_read.offset;
        len = req->ll_read.len;

        if (len > sizeof(recv_buffer)) {
            err = proxy_log(LOG_ERR, ENOBUFS, "Attempt to read too much data");
        } else {
            err = ceph_ll_read(cmount, fh, offset, len, recv_buffer);
            TRACE("ceph_ll_read(%p, %p, %ld, %lu) -> %d", cmount, fh, offset,
                  len, err);

            if (err >= 0) {
                CEPH_BUFF_ADD(ans, recv_buffer, err);
            }
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_write(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_write, ans, 0);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    uint64_t len;
    int64_t offset;
    int32_t err;

    err = ptr_check(&client->random, req->ll_write.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_write.fh, (void **)&fh);
    }
    if (err >= 0) {
        offset = req->ll_write.offset;
        len = req->ll_write.len;

        err = ceph_ll_write(cmount, fh, offset, len, data);
        TRACE("ceph_ll_write(%p, %p, %ld, %lu) -> %d", cmount, fh, offset, len,
              err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_link(proxy_client_t *client, proxy_req_t *req, const void *data,
                   int32_t data_size)
{
    CEPH_DATA(ceph_ll_link, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *parent, *inode;
    const char *name;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_link.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_link.inode, (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_link.parent, (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_link.userperm, (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_link, name, data);

        err = ceph_ll_link(cmount, inode, parent, name, perms);
        TRACE("ceph_ll_link(%p, %p, %p, '%s', %p) -> %d", cmount, inode, parent,
              name, perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_unlink(proxy_client_t *client, proxy_req_t *req, const void *data,
                     int32_t data_size)
{
    CEPH_DATA(ceph_ll_unlink, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *parent;
    const char *name;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_unlink.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_unlink.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_unlink.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_unlink, name, data);

        err = ceph_ll_unlink(cmount, parent, name, perms);
        TRACE("ceph_ll_unlink(%p, %p, '%s', %p) -> %d", cmount, parent, name,
              perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_getattr(proxy_client_t *client, proxy_req_t *req,
                      const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_getattr, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    UserPerm *perms;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_getattr.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_getattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_getattr.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        want = req->ll_getattr.want;
        flags = req->ll_getattr.flags;

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_getattr(cmount, inode, &stx, want, flags, perms);
        TRACE("ceph_ll_getattr(%p, %p, %x, %x, %p) -> %d", cmount, inode, want,
              flags, perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_setattr(proxy_client_t *client, proxy_req_t *req,
                      const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_setattr, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    UserPerm *perms;
    int32_t mask, err;

    err = ptr_check(&client->random, req->ll_setattr.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_setattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_setattr.userperm, (void **)&perms);
    }
    if (err >= 0) {
        mask = req->ll_setattr.mask;

        err = ceph_ll_setattr(cmount, inode, (void *)data, mask, perms);
        TRACE("ceph_ll_setattr(%p, %p, %x, %p) -> %d", cmount, inode, mask,
              perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_fallocate(proxy_client_t *client, proxy_req_t *req,
                        const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_fallocate, ans, 0);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    int64_t offset, len;
    mode_t mode;
    int32_t err;

    err = ptr_check(&client->random, req->ll_fallocate.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_fallocate.fh, (void **)&fh);
    }
    if (err >= 0) {
        mode = req->ll_fallocate.mode;
        offset = req->ll_fallocate.offset;
        len = req->ll_fallocate.length;

        err = ceph_ll_fallocate(cmount, fh, mode, offset, len);
        TRACE("ceph_ll_fallocate(%p, %p, %o, %ld, %lu) -> %d", cmount, fh, mode,
              offset, len, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_fsync(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_fsync, ans, 0);
    struct ceph_mount_info *cmount;
    struct Fh *fh;
    int32_t dataonly, err;

    err = ptr_check(&client->random, req->ll_fsync.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_fsync.fh, (void **)&fh);
    }
    if (err >= 0) {
        dataonly = req->ll_fsync.dataonly;

        err = ceph_ll_fsync(cmount, fh, dataonly);
        TRACE("ceph_ll_fsync(%p, %p, %d) -> %d", cmount, fh, dataonly, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_listxattr(proxy_client_t *client, proxy_req_t *req,
                        const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_listxattr, ans, 1);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    UserPerm *perms;
    size_t size;
    int32_t err;

    err = ptr_check(&client->random, req->ll_listxattr.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_listxattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_listxattr.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        size = req->ll_listxattr.size;

        if (size > sizeof(recv_buffer)) {
            err = proxy_log(LOG_ERR, ENOBUFS, "Attempt to read too much data");
        } else {
            err = ceph_ll_listxattr(cmount, inode, recv_buffer, size, &size,
                                    perms);
            TRACE("ceph_ll_listxattr(%p, %p, %lu, %p) -> %d", cmount, inode,
                  size, perms, err);

            if (err >= 0) {
                ans.size = size;
                CEPH_BUFF_ADD(ans, recv_buffer, size);
            }
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_getxattr(proxy_client_t *client, proxy_req_t *req,
                       const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_getxattr, ans, 1);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    const char *name;
    UserPerm *perms;
    size_t size;
    int32_t err;

    err = ptr_check(&client->random, req->ll_getxattr.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_getxattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_getxattr.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        size = req->ll_getxattr.size;
        name = CEPH_STR_GET(req->ll_getxattr, name, data);

        if (size > sizeof(recv_buffer)) {
            err = proxy_log(LOG_ERR, ENOBUFS, "Attempt to read too much data");
        } else {
            err = ceph_ll_getxattr(cmount, inode, name, recv_buffer, size,
                                   perms);
            TRACE("ceph_ll_getxattr(%p, %p, '%s', %p) -> %d", cmount, inode,
                  name, perms, err);

            if (err >= 0) {
                CEPH_BUFF_ADD(ans, recv_buffer, err);
            }
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_setxattr(proxy_client_t *client, proxy_req_t *req,
                       const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_setxattr, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    const char *name, *value;
    UserPerm *perms;
    size_t size;
    int32_t flags, err;

    err = ptr_check(&client->random, req->ll_setxattr.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_setxattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_setxattr.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_setxattr, name, data);
        value = data + req->ll_setxattr.name;
        size = req->ll_setxattr.size;
        flags = req->ll_setxattr.flags;

        err = ceph_ll_setxattr(cmount, inode, name, value, size, flags, perms);
        TRACE("ceph_ll_setxattr(%p, %p, '%s', %p, %x, %p) -> %d", cmount, inode,
              name, value, flags, perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_removexattr(proxy_client_t *client, proxy_req_t *req,
                          const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_removexattr, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    const char *name;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_removexattr.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_removexattr.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_removexattr.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_removexattr, name, data);

        err = ceph_ll_removexattr(cmount, inode, name, perms);
        TRACE("ceph_ll_removexattr(%p, %p, '%s', %p) -> %d", cmount, inode,
              name, perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_readlink(proxy_client_t *client, proxy_req_t *req,
                       const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_readlink, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    UserPerm *perms;
    size_t size;
    int32_t err;

    err = ptr_check(&client->random, req->ll_readlink.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_readlink.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_readlink.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        size = req->ll_readlink.size;

        if (size > sizeof(recv_buffer)) {
            err = proxy_log(LOG_ERR, ENOBUFS, "Attempt to read too much data");
        } else {
            err = ceph_ll_readlink(cmount, inode, recv_buffer, size, perms);
            TRACE("ceph_ll_readlink(%p, %p, %p) -> %d", cmount, inode, perms,
                  err);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_symlink(proxy_client_t *client, proxy_req_t *req,
                      const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_symlink, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *parent, *inode;
    UserPerm *perms;
    const char *name, *value;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_symlink.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_symlink.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_symlink.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_symlink, name, data);
        value = CEPH_STR_GET(req->ll_symlink, target, data + req->ll_symlink.name);
        want = req->ll_symlink.want;
        flags = req->ll_symlink.flags;

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_symlink(cmount, parent, name, value, &inode, &stx, want,
                            flags, perms);
        TRACE("ceph_ll_symlink(%p, %p, '%s', '%s', %p, %x, %x, %p) -> %d",
              cmount, parent, name, value, inode, want, flags, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_opendir(proxy_client_t *client, proxy_req_t *req,
                      const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_opendir, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *inode;
    struct ceph_dir_result *dirp;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_opendir.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_opendir.inode,
                        (void **)&inode);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_opendir.userperm,
                        (void **)&perms);
    }

    if (err >= 0) {
        err = ceph_ll_opendir(cmount, inode, &dirp, perms);
        TRACE("ceph_ll_opendir(%p, %p, %p, %p) -> %d", cmount, inode, dirp,
              perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, dirp, &ans.dir);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_mkdir(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_mkdir, ans, 1);
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    struct Inode *parent, *inode;
    const char *name;
    UserPerm *perms;
    mode_t mode;
    uint32_t want, flags;
    int32_t err;

    err = ptr_check(&client->random, req->ll_mkdir.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_mkdir.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_mkdir.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        mode = req->ll_mkdir.mode;
        want = req->ll_mkdir.want;
        flags = req->ll_mkdir.flags;
        name = CEPH_STR_GET(req->ll_mkdir, name, data);

        CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

        err = ceph_ll_mkdir(cmount, parent, name, mode, &inode, &stx, want,
                            flags, perms);
        TRACE("ceph_ll_mkdir(%p, %p, '%s', %o, %p, %x, %x, %p) -> %d", cmount,
              parent, name, mode, inode, want, flags, perms, err);

        if (err >= 0) {
            err = ptr_checksum(&client->random, inode, &ans.inode);
        }
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_rmdir(proxy_client_t *client, proxy_req_t *req, const void *data,
                    int32_t data_size)
{
    CEPH_DATA(ceph_ll_rmdir, ans, 0);
    struct ceph_mount_info *cmount;
    struct Inode *parent;
    const char *name;
    UserPerm *perms;
    int32_t err;

    err = ptr_check(&client->random, req->ll_rmdir.cmount, (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_rmdir.parent,
                        (void **)&parent);
    }
    if (err >= 0) {
        err = ptr_check(&global_random, req->ll_rmdir.userperm,
                        (void **)&perms);
    }
    if (err >= 0) {
        name = CEPH_STR_GET(req->ll_rmdir, name, data);

        err = ceph_ll_rmdir(cmount, parent, name, perms);
        TRACE("ceph_ll_rmdir(%p, %p, '%s', %p) -> %d", cmount, parent, name,
              perms, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static int32_t
libcephfsd_ll_releasedir(proxy_client_t *client, proxy_req_t *req,
                         const void *data, int32_t data_size)
{
    CEPH_DATA(ceph_ll_releasedir, ans, 0);
    struct ceph_mount_info *cmount;
    struct ceph_dir_result *dirp;
    int32_t err;

    err = ptr_check(&client->random, req->ll_releasedir.cmount,
                    (void **)&cmount);
    if (err >= 0) {
        err = ptr_check(&client->random, req->ll_releasedir.dir,
                        (void **)&dirp);
    }

    if (err >= 0) {
        err = ceph_ll_releasedir(cmount, dirp);
        TRACE("ceph_ll_releasedir(%p, %p) -> %d", cmount, dirp, err);
    }

    return CEPH_COMPLETE(client, err, ans);
}

static proxy_handler_t libcephfsd_handlers[LIBCEPHFSD_OP_TOTAL_OPS] = {
    [LIBCEPHFSD_OP_VERSION] = libcephfsd_version,
    [LIBCEPHFSD_OP_USERPERM_NEW] = libcephfsd_userperm_new,
    [LIBCEPHFSD_OP_USERPERM_DESTROY] = libcephfsd_userperm_destroy,
    [LIBCEPHFSD_OP_CREATE] = libcephfsd_create,
    [LIBCEPHFSD_OP_RELEASE] = libcephfsd_release,
    [LIBCEPHFSD_OP_CONF_READ_FILE] = libcephfsd_conf_read_file,
    [LIBCEPHFSD_OP_CONF_GET] = libcephfsd_conf_get,
    [LIBCEPHFSD_OP_CONF_SET] = libcephfsd_conf_set,
    [LIBCEPHFSD_OP_INIT] = libcephfsd_init,
    [LIBCEPHFSD_OP_SELECT_FILESYSTEM] = libcephfsd_select_filesystem,
    [LIBCEPHFSD_OP_MOUNT] = libcephfsd_mount,
    [LIBCEPHFSD_OP_UNMOUNT] = libcephfsd_unmount,
    [LIBCEPHFSD_OP_LL_STATFS] = libcephfsd_ll_statfs,
    [LIBCEPHFSD_OP_LL_LOOKUP] = libcephfsd_ll_lookup,
    [LIBCEPHFSD_OP_LL_LOOKUP_INODE] = libcephfsd_ll_lookup_inode,
    [LIBCEPHFSD_OP_LL_LOOKUP_ROOT] = libcephfsd_ll_lookup_root,
    [LIBCEPHFSD_OP_LL_PUT] = libcephfsd_ll_put,
    [LIBCEPHFSD_OP_LL_WALK] = libcephfsd_ll_walk,
    [LIBCEPHFSD_OP_CHDIR] = libcephfsd_chdir,
    [LIBCEPHFSD_OP_GETCWD] = libcephfsd_getcwd,
    [LIBCEPHFSD_OP_READDIR] = libcephfsd_readdir,
    [LIBCEPHFSD_OP_REWINDDIR] = libcephfsd_rewinddir,
    [LIBCEPHFSD_OP_LL_OPEN] = libcephfsd_ll_open,
    [LIBCEPHFSD_OP_LL_CREATE] = libcephfsd_ll_create,
    [LIBCEPHFSD_OP_LL_MKNOD] = libcephfsd_ll_mknod,
    [LIBCEPHFSD_OP_LL_CLOSE] = libcephfsd_ll_close,
    [LIBCEPHFSD_OP_LL_RENAME] = libcephfsd_ll_rename,
    [LIBCEPHFSD_OP_LL_LSEEK] = libcephfsd_ll_lseek,
    [LIBCEPHFSD_OP_LL_READ] = libcephfsd_ll_read,
    [LIBCEPHFSD_OP_LL_WRITE] = libcephfsd_ll_write,
    [LIBCEPHFSD_OP_LL_LINK] = libcephfsd_ll_link,
    [LIBCEPHFSD_OP_LL_UNLINK] = libcephfsd_ll_unlink,
    [LIBCEPHFSD_OP_LL_GETATTR] = libcephfsd_ll_getattr,
    [LIBCEPHFSD_OP_LL_SETATTR] = libcephfsd_ll_setattr,
    [LIBCEPHFSD_OP_LL_FALLOCATE] = libcephfsd_ll_fallocate,
    [LIBCEPHFSD_OP_LL_FSYNC] = libcephfsd_ll_fsync,
    [LIBCEPHFSD_OP_LL_LISTXATTR] = libcephfsd_ll_listxattr,
    [LIBCEPHFSD_OP_LL_GETXATTR] = libcephfsd_ll_getxattr,
    [LIBCEPHFSD_OP_LL_SETXATTR] = libcephfsd_ll_setxattr,
    [LIBCEPHFSD_OP_LL_REMOVEXATTR] = libcephfsd_ll_removexattr,
    [LIBCEPHFSD_OP_LL_READLINK] = libcephfsd_ll_readlink,
    [LIBCEPHFSD_OP_LL_SYMLINK] = libcephfsd_ll_symlink,
    [LIBCEPHFSD_OP_LL_OPENDIR] = libcephfsd_ll_opendir,
    [LIBCEPHFSD_OP_LL_MKDIR] = libcephfsd_ll_mkdir,
    [LIBCEPHFSD_OP_LL_RMDIR] = libcephfsd_ll_rmdir,
    [LIBCEPHFSD_OP_LL_RELEASEDIR] = libcephfsd_ll_releasedir,
};

static void
client_cmd_version(proxy_client_t *client)
{
    const char *text;
    int32_t major, minor, patch;

    text = ceph_version(&major, &minor, &patch);

    client_write(client, "libcephfs version %d.%d.%d (%s)\n", major, minor,
                 patch, text);
}

static client_command_t client_commands[] = {
    { "version", client_cmd_version },
    { NULL, NULL }
};

static void
serve_text(proxy_client_t *client)
{
    client_command_t *cmd;
    char *line;
    int32_t err;

    err = client_init(client, 4096);
    if (err < 0) {
        return;
    }

    client_write(client, "version %d.%d\n", LIBCEPHFSD_MAJOR, LIBCEPHFSD_MINOR);

    proxy_log_register(&client->log_handler, client_log_handler);

    while ((err = proxy_buffer_read_line(&client->buffer_read, &line)) >= 0) {
        while ((err > 0) && isspace(line[err - 1])) {
            err--;
        }
        line[err] = 0;
        while (isspace(*line)) {
            line++;
        }
        if (*line == 0) {
            continue;
        }

        if (strcmp(line, "quit") == 0) {
            break;
        }

        for (cmd = client_commands; cmd->name != NULL; cmd++) {
            if (strcmp(cmd->name, line) == 0) {
                cmd->handler(client);
                break;
            }
        }
        if (cmd->name == NULL) {
            proxy_log(LOG_ERR, EINVAL, "Unknown command");
        }
    }

    proxy_log_deregister(&client->log_handler);

    client_destroy(client);
}

static void
serve_binary(proxy_client_t *client)
{
    proxy_req_t req;
    CEPH_DATA(hello, ans, 0);
    struct iovec req_iov[2];
    int32_t err;

    ans.major = LIBCEPHFSD_MAJOR;
    ans.minor = LIBCEPHFSD_MINOR;
    err = proxy_link_send(client->sd, ans_iov, ans_count);
    if (err < 0) {
        return;
    }

    while (true) {
        req_iov[0].iov_base = &req;
        req_iov[0].iov_len = sizeof(req);
        req_iov[1].iov_base = recv_buffer;
        req_iov[1].iov_len = sizeof(recv_buffer);

        err = proxy_link_req_recv(client->sd, req_iov, 2);
        if (err <= 0) {
            break;
        }

        if (req.header.op >= LIBCEPHFSD_OP_TOTAL_OPS) {
            err = send_error(client, -ENOSYS);
        } else if (libcephfsd_handlers[req.header.op] == NULL) {
            err = send_error(client, -EOPNOTSUPP);
        } else {
//            printf("$$$$ Serving request %d\n", req.header.op);
            err = libcephfsd_handlers[req.header.op](client, &req, recv_buffer,
                                                     req.header.data_len);
        }

        if (err < 0) {
            break;
        }
    }
}

static void
serve_connection(proxy_worker_t *worker)
{
    CEPH_DATA(hello, req, 0);
    proxy_client_t *client;
    int32_t err;

    client = container_of(worker, proxy_client_t, worker);

    err = proxy_link_recv(client->sd, req_iov, req_count);
    if (err >= 0) {
        if (be32toh(req.id) == LIBCEPHFS_TEXT_CLIENT) {
            serve_text(client);
        } else if (req.id == LIBCEPHFS_LIB_CLIENT) {
            serve_binary(client);
        } else {
            proxy_log(LOG_ERR, EINVAL, "Invalid client initial message");
        }
    }

    close(client->sd);
}

static void
destroy_connection(proxy_worker_t *worker)
{
    proxy_client_t *client;

    client = container_of(worker, proxy_client_t, worker);

    proxy_free(client);
}

static int32_t
accept_connection(proxy_link_t *link, int32_t sd)
{
    proxy_server_t *server;
    proxy_client_t *client;
    int32_t err;

    server = container_of(link, proxy_server_t, link);

    client = proxy_malloc(sizeof(proxy_client_t));
    if (client == NULL) {
        err = -ENOMEM;
        goto failed_close;
    }

    random_init(&client->random);
    client->sd = sd;
    client->link = link;

    err = proxy_manager_launch(server->manager, &client->worker,
                               serve_connection, destroy_connection);
    if (err < 0) {
        goto failed_memory;
    }

    return 0;

failed_memory:
    proxy_free(client);

failed_close:
    close(sd);

    return err;
}

static bool
check_stop(proxy_link_t *link)
{
    proxy_server_t *server;

    server = container_of(link, proxy_server_t, link);

    return proxy_manager_stop(server->manager);
}

static int32_t
server_main(proxy_manager_t *manager)
{
    proxy_server_t server;
    proxy_t *proxy;

    proxy = container_of(manager, proxy_t, manager);

    server.manager = manager;

    return proxy_link_server(&server.link, proxy->socket_path,
                             accept_connection, check_stop);
}

static void
log_print(proxy_log_handler_t *handler, int32_t level, int32_t err,
          const char *msg)
{
    printf("[%d] %s\n", level, msg);
}

int32_t
main(int32_t argc, char *argv[])
{
    struct timespec now;
    proxy_t proxy;
    int32_t err;

    clock_gettime(CLOCK_MONOTONIC, &now);
    srand(now.tv_nsec);

    random_init(&global_random);

    proxy_log_register(&proxy.log_handler, log_print);

    proxy.socket_path = "/tmp/libcephfsd.sock";
    if (argc > 1) {
        proxy.socket_path = argv[1];
    }

    err = proxy_manager_run(&proxy.manager, server_main);

    proxy_log_deregister(&proxy.log_handler);

    return err < 0 ? 1 : 0;
}