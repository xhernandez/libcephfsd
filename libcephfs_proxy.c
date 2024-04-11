
#include <cephfs/libcephfs.h>

#include "proxy_log.h"
#include "proxy_helpers.h"
#include "proxy_requests.h"

typedef struct ceph_dentry {
    struct ceph_dentry *next;
    struct Inode *parent;
    struct Inode *inode;
    uint32_t len;
    char name[];
} ceph_dentry_t;

struct Inode {
    struct ceph_statx stx;
    struct Inode *next;
    uint64_t inode;
    uint32_t refs;
};

struct ceph_mount_info {
    proxy_link_t link;
    uint64_t cmount;
    struct Inode *cwd_inode;
    struct Inode *root_inode;
    bool good;
    char cwd[PATH_MAX];
};

static struct ceph_mount_info global_cmount = { .good = false };

static bool
client_stop(proxy_link_t *link)
{
    return false;
}

static int32_t
proxy_connect(proxy_link_t *link)
{
    CEPH_REQ(hello, req, 0, ans, 0);
    int32_t sd, err;

    sd = proxy_link_client(link, "/tmp/libcephfsd.sock", client_stop);
    if (sd < 0) {
        return sd;
    }

    req.id = LIBCEPHFS_LIB_CLIENT;
    err = proxy_link_send(sd, req_iov, 1);
    if (err < 0) {
        goto failed;
    }
    err = proxy_link_recv(sd, ans_iov, 1);
    if (err < 0) {
        goto failed;
    }

    proxy_log(LOG_INFO, 0, "Connected to libcephfsd version %d.%d", ans.major,
              ans.minor);

    if ((ans.major != LIBCEPHFSD_MAJOR) || (ans.minor != LIBCEPHFSD_MINOR)) {
        err = proxy_log(LOG_ERR, ENOTSUP, "Version not supported");
        goto failed;
    }

    return sd;

failed:
    proxy_link_close(link);

    return err;
}

static void
proxy_disconnect(proxy_link_t *link)
{
    proxy_link_close(link);
}

static int32_t
proxy_global_connect(void)
{
    int32_t err;

    err = 0;

    if (!global_cmount.good) {
        err = proxy_connect(&global_cmount.link);
        if (err >= 0) {
            global_cmount.good = true;
        }
    }

    return err;
}

static int32_t
proxy_check(struct ceph_mount_info *cmount, int32_t err, int32_t result)
{
    if (err < 0) {
        proxy_disconnect(&cmount->link);
        cmount->good = false;
        proxy_log(LOG_ERR, err, "Disconnected from libcephfsd");

        return err;
    }

    return result;
}

#define INODE_HASH_TABLE_SIZE 65537
#define DENTRY_HASH_TABLE_SIZE 65537

static struct Inode *inode_table[INODE_HASH_TABLE_SIZE] = {};
static ceph_dentry_t *dentry_table[DENTRY_HASH_TABLE_SIZE] = {};

static struct Inode *
inode_ref(struct Inode *inode)
{
    inode->refs++;

    return inode;
}

static bool
inode_unref(struct Inode *inode)
{
    struct Inode **pinode;
    uint64_t ino;

    if (--inode->refs > 0) {
        return false;
    }

    ino = inode->stx.stx_ino % INODE_HASH_TABLE_SIZE;
    pinode = &inode_table[ino];
    while (*pinode != inode) {
        pinode = &(*pinode)->next;
    }

    *pinode = inode->next;

    return true;
}

static struct Inode *
inode_lookup(uint64_t ino)
{
    struct Inode *inode;

    ino %= INODE_HASH_TABLE_SIZE;
    for (inode = inode_table[ino]; inode != NULL; inode = inode->next) {
        if (inode->stx.stx_ino == ino) {
            return inode_ref(inode);
        }
    }

    return NULL;
}

static void
inode_destroy(struct Inode *inode)
{
    proxy_free(inode);
}

static void
inode_update(struct Inode *inode, struct ceph_statx *stx)
{
    inode->stx.stx_mask |= stx->stx_mask;
    inode->stx.stx_blksize = stx->stx_blksize;
    inode->stx.stx_dev = stx->stx_dev;
    if ((stx->stx_mask & CEPH_STATX_MODE) != 0) {
        inode->stx.stx_mode = stx->stx_mode;
    }
    if ((stx->stx_mask & CEPH_STATX_NLINK) != 0) {
        inode->stx.stx_nlink = stx->stx_nlink;
    }
    if ((stx->stx_mask & CEPH_STATX_UID) != 0) {
        inode->stx.stx_uid = stx->stx_uid;
    }
    if ((stx->stx_mask & CEPH_STATX_GID) != 0) {
        inode->stx.stx_gid = stx->stx_gid;
    }
    if ((stx->stx_mask & CEPH_STATX_RDEV) != 0) {
        inode->stx.stx_rdev = stx->stx_rdev;
    }
    if ((stx->stx_mask & CEPH_STATX_ATIME) != 0) {
        inode->stx.stx_atime = stx->stx_atime;
    }
    if ((stx->stx_mask & CEPH_STATX_MTIME) != 0) {
        inode->stx.stx_mtime = stx->stx_mtime;
    }
    if ((stx->stx_mask & CEPH_STATX_CTIME) != 0) {
        inode->stx.stx_ctime = stx->stx_ctime;
    }
    if ((stx->stx_mask & CEPH_STATX_INO) != 0) {
        inode->stx.stx_ino = stx->stx_ino;
    }
    if ((stx->stx_mask & CEPH_STATX_SIZE) != 0) {
        inode->stx.stx_size = stx->stx_size;
    }
    if ((stx->stx_mask & CEPH_STATX_BLOCKS) != 0) {
        inode->stx.stx_blocks = stx->stx_blocks;
    }
    if ((stx->stx_mask & CEPH_STATX_BTIME) != 0) {
        inode->stx.stx_btime = stx->stx_btime;
    }
    if ((stx->stx_mask & CEPH_STATX_VERSION) != 0) {
        inode->stx.stx_version = stx->stx_version;
    }
}

static int32_t
inode_create(struct ceph_mount_info *cmount, struct Inode **pinode,
             uint64_t ceph_inode, struct ceph_statx *stx)
{
    struct Inode *inode;
    uint64_t ino;

    if ((stx->stx_mask & CEPH_STATX_INO) == 0) {
        ceph_ll_put(cmount, value_ptr(ceph_inode));
        return proxy_log(LOG_ERR, EINVAL, "No inode number present");
    }

    ino = stx->stx_ino;

    inode = inode_lookup(ino);
    if (inode == NULL) {
        inode = proxy_malloc(sizeof(struct Inode));
        if (inode == NULL) {
            ceph_ll_put(cmount, value_ptr(ceph_inode));
            return -ENOMEM;
        }

        inode->inode = ceph_inode;
        inode->refs = 1;
        memset(&inode->stx, 0, sizeof(inode->stx));

        ino %= INODE_HASH_TABLE_SIZE;
        inode->next = inode_table[ino];
        inode_table[ino] = inode;
    }

    inode_update(inode, stx);

    *pinode = inode;

    return 0;
}

static int32_t
inode_create_ino(struct ceph_mount_info *cmount, struct Inode **pinode,
                 uint64_t ceph_inode, uint64_t ino)
{
    struct ceph_statx stx;

    /* TODO: stx_blksize and stx_dev will be incorrect. */
    memset(&stx, 0, sizeof(stx));
    stx.stx_mask = CEPH_STATX_INO;
    stx.stx_ino = ino;

    return inode_create(cmount, pinode, ceph_inode, &stx);
}

#define MURMUR_SCRAMBLE(_in, _h1, _h2, _c1, _c2, _shift1, _shift2, _mul, _val) \
    ({ \
        uint64_t __out = (_in); \
        if (__out != 0) { \
            __out *= _c1; \
            __out = (__out << _shift1) | (__out >> ((64 - _shift1) & 63)); \
            __out *= _c2; \
        } \
        __out ^= _h1; \
        __out = (__out << _shift2) | (__out >> ((64 - _shift2) & 63)); \
        __out += _h2; \
        __out *= _mul; \
        __out += _val; \
        __out; \
    })

#define MURMUR_FMIX(_val) \
    ({ \
        _val ^= _val >> 33; \
        _val *= 0xff51afd7ed558ccdULL; \
        _val ^= _val >> 33; \
        _val *= 0xc4ceb9fe1a85ec53ULL; \
        _val ^= _val >> 33; \
        _val; \
    })

/* Implementation of MurmurHash3 (x64/128) */
static uint64_t
murmurhash3_x64_64(const char *text, uint32_t len)
{
    const uint64_t *blocks;
    uint64_t h1, h2, c1, c2, mask;
    uint32_t i, count;

    blocks = (const uint64_t *)text;
    count = len / 16;

    h1 = 0xd304bfad9d308087ULL;
    h2 = 0x4542871a0afb8fe3ULL;

    c1 = 0x87c37b91114253d5ULL;
    c2 = 0x4cf5ad432745937fULL;

    for (i = 0; i < count; i++) {
        h1 = MURMUR_SCRAMBLE(*blocks++, h1, h2, c1, c2, 31, 27, 5, 0x52dce729);
        h2 = MURMUR_SCRAMBLE(*blocks++, h2, h1, c2, c1, 33, 31, 5, 0x38495ab5);
    }

    h1 ^= len;
    h2 ^= len;

    mask = (1ULL << ((len & 7) * 8)) - 1ULL;
    if ((len & 8) == 0) {
        h1 = MURMUR_SCRAMBLE(*blocks & mask, h1, h2, c1, c2, 29, 0, 1, 0);
        h2 += h1;
    } else {
        h1 = MURMUR_SCRAMBLE(*blocks++, h1, h2, c1, c2, 29, 0, 1, 0);
        h2 = MURMUR_SCRAMBLE(*blocks & mask, h2, h1, c2, c1, 33, 0, 1, 0);
    }

    h1 = MURMUR_FMIX(h1);
    h2 = MURMUR_FMIX(h2);

    h1 += h2;
    h2 += h1;

    return h1 ^ h2;
}

static ceph_dentry_t *
dentry_lookup(struct Inode *parent, const char *name)
{
    ceph_dentry_t *dentry;
    uint64_t hash;
    uint32_t len;

    len = strlen(name) + 1;

    hash = ptr_value(parent) ^ murmurhash3_x64_64(name, len);
    hash %= DENTRY_HASH_TABLE_SIZE;
    for (dentry = dentry_table[hash]; dentry != NULL; dentry = dentry->next) {
        if ((dentry->parent == parent) && (dentry->len == len) &&
            (memcmp(dentry->name, name, len) == 0)) {
            return dentry;
        }
    }

    return NULL;
}

static void
dentry_destroy(struct ceph_mount_info *cmount, ceph_dentry_t *dentry)
{
    ceph_ll_put(cmount, dentry->inode);
    ceph_ll_put(cmount, dentry->parent);
    proxy_free(dentry);
}

static int32_t
dentry_create(struct ceph_mount_info *cmount, struct Inode *parent,
              struct Inode *inode, const char *name)
{
    ceph_dentry_t **pdentry, *dentry;
    uint64_t hash;
    uint32_t len;

    len = strlen(name) + 1;
    hash = ptr_value(parent) ^ murmurhash3_x64_64(name, len);
    hash %= DENTRY_HASH_TABLE_SIZE;
    for (pdentry = &dentry_table[hash]; (dentry = *pdentry) != NULL;
         pdentry = &dentry->next) {
        if ((dentry->parent == parent) && (dentry->len == len) &&
            (memcmp(dentry->name, name, len) == 0)) {
            if (dentry->inode != inode) {
                ceph_ll_put(cmount, dentry->inode);
                dentry->inode = inode_ref(inode);
            }

            return 0;
        }
    }

    dentry = proxy_malloc(sizeof(ceph_dentry_t) + len);
    if (dentry == NULL) {
        return -ENOMEM;
    }

    dentry->parent = inode_ref(parent);
    dentry->inode = inode_ref(inode);
    dentry->len = len;
    memcpy(dentry->name, name, len);

    dentry->next = dentry_table[hash];
    dentry_table[hash] = dentry;

    return 0;
}

#define CEPH_RUN(_cmount, _op, _req, _ans) \
    ({ \
        int32_t __err = CEPH_CALL((_cmount)->link.sd, _op, _req, _ans); \
        __err = proxy_check(_cmount, __err, (_ans).header.result); \
        __err; \
    })

#define CEPH_PROCESS(_cmount, _op, _req, _ans) \
    ({ \
        int32_t __err = -ENOTCONN; \
        if ((_cmount)->good) { \
            (_req).cmount = (_cmount)->cmount; \
            __err = CEPH_RUN(_cmount, _op, _req, _ans); \
        } \
        __err; \
    })

__public int
ceph_chdir(struct ceph_mount_info *cmount, const char *path)
{
    CEPH_REQ(ceph_chdir, req, 1, ans, 1);
    int32_t err;

    if (strcmp(cmount->cwd, path) == 0) {
        return 0;
    }

    req.inode = cmount->cwd_inode == NULL ? 0 : cmount->cwd_inode->inode;
    CEPH_STR_ADD(req, path, path);

    CEPH_BUFF_ADD(ans, cmount->cwd, sizeof(cmount->cwd));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CHDIR, req, ans);
    if (err >= 0) {
        cmount->cwd_inode = NULL;
    }

    return err;
}

__public int
ceph_conf_get(struct ceph_mount_info *cmount, const char *option, char *buf,
              size_t len)
{
    CEPH_REQ(ceph_conf_get, req, 1, ans, 1);

    req.size = len;

    CEPH_STR_ADD(req, option, option);
    CEPH_BUFF_ADD(ans, buf, len);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_GET, req, ans);
}

__public int
ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path_list)
{
    CEPH_REQ(ceph_conf_read_file, req, 1, ans, 0);

    CEPH_STR_ADD(req, path, path_list);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_READ_FILE, req, ans);
}

__public int
ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
              const char *value)
{
    CEPH_REQ(ceph_conf_set, req, 2, ans, 0);

    CEPH_STR_ADD(req, option, option);
    CEPH_STR_ADD(req, value, value);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_SET, req, ans);
}

__public int
ceph_create(struct ceph_mount_info **cmount, const char *const id)
{
    CEPH_REQ(ceph_create, req, 1, ans, 0);
    struct ceph_mount_info *ceph_mount;
    int32_t sd, err;

    ceph_mount = proxy_malloc(sizeof(struct ceph_mount_info));
    if (ceph_mount == NULL) {
        return -ENOMEM;
    }

    err = proxy_connect(&ceph_mount->link);
    if (err < 0) {
        goto failed;
    }
    sd = err;

    CEPH_STR_ADD(req, id, id);

    err = CEPH_CALL(sd, LIBCEPHFSD_OP_CREATE, req, ans);
    if ((err < 0) || ((err = ans.header.result) < 0)) {
        goto failed_link;
    }

    ceph_mount->cmount = ans.cmount;
    ceph_mount->good = true;
    ceph_mount->cwd_inode = NULL;
    *ceph_mount->cwd = 0;
    ceph_mount->root_inode = NULL;

    *cmount = ceph_mount;

    return 0;

failed_link:
    proxy_disconnect(&ceph_mount->link);

failed:
    proxy_free(ceph_mount);

    return err;
}

__public const char *
ceph_getcwd(struct ceph_mount_info *cmount)
{
    CEPH_REQ(ceph_getcwd, req, 0, ans, 1);
    int32_t err;

    if (*cmount->cwd != 0) {
        return cmount->cwd;
    }

    CEPH_BUFF_ADD(ans, cmount->cwd, sizeof(cmount->cwd));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_GETCWD, req, ans);
    if (err >= 0) {
        return cmount->cwd;
    }

    errno = -err;

    return NULL;
}

__public int
ceph_init(struct ceph_mount_info *cmount)
{
    CEPH_REQ(ceph_init, req, 0, ans, 0);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_INIT, req, ans);
}

__public int
ceph_ll_close(struct ceph_mount_info *cmount, struct Fh *filehandle)
{
    CEPH_REQ(ceph_ll_close, req, 0, ans, 0);

    req.fh = ptr_value(filehandle);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CLOSE, req, ans);
}

__public int
ceph_ll_create(struct ceph_mount_info *cmount, Inode *parent, const char *name,
               mode_t mode, int oflags, Inode **outp, Fh **fhp,
               struct ceph_statx *stx, unsigned want, unsigned lflags,
               const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_create, req, 1, ans, 1);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.parent = parent->inode;
    req.mode = mode;
    req.oflags = oflags;
    req.want = want | CEPH_STATX_INO;
    req.flags = lflags;

    CEPH_STR_ADD(req, name, name);
    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CREATE, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, outp, ans.inode, stx);
        if (err >= 0) {
            err = dentry_create(cmount, parent, *outp, name);
        }
        /* TODO: leak of ans.fh in case of error */
        if (err >= 0) {
            *fhp = value_ptr(ans.fh);
        }
    }

    return err;
}

__public int
ceph_ll_fallocate(struct ceph_mount_info *cmount, struct Fh *fh, int mode,
                  int64_t offset, int64_t length)
{
    CEPH_REQ(ceph_ll_fallocate, req, 0, ans, 0);

    req.fh = ptr_value(fh);
    req.mode = mode;
    req.offset = offset;
    req.length = length;

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FALLOCATE, req, ans);
}

__public int
ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh, int syncdataonly)
{
    CEPH_REQ(ceph_ll_fsync, req, 0, ans, 0);

    req.fh = ptr_value(fh);
    req.dataonly = syncdataonly;

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FSYNC, req, ans);
}

__public int
ceph_ll_getattr(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_statx *stx, unsigned int want, unsigned int flags,
                const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_getattr, req, 0, ans, 1);
    int32_t err;

    /* TODO: perms shouldn't be ignored. */
    if ((in->stx.stx_mask & want) == want) {
        memcpy(stx, &in->stx, sizeof(*stx));
        return 0;
    }

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.want = want | CEPH_STATX_INO;
    req.flags = flags;

    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETATTR, req, ans);
    if (err >= 0) {
        inode_update(in, stx);
    }

    return err;
}

__public int
ceph_ll_getxattr(struct ceph_mount_info *cmount, struct Inode *in,
                 const char *name, void *value, size_t size,
                 const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_getxattr, req, 1, ans, 1);

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.size = size;
    CEPH_STR_ADD(req, name, name);

    CEPH_BUFF_ADD(ans, value, size);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETXATTR, req, ans);
}

__public int
ceph_ll_link(struct ceph_mount_info *cmount, struct Inode *in,
             struct Inode *newparent, const char *name, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_link, req, 1, ans, 0);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.parent = newparent->inode;
    CEPH_STR_ADD(req, name, name);

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LINK, req, ans);
    if (err >= 0) {
        err = dentry_create(cmount, newparent, in, name);
    }

    return err;
}

__public int
ceph_ll_listxattr(struct ceph_mount_info *cmount, struct Inode *in, char *list,
                  size_t buf_size, size_t *list_size, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_listxattr, req, 0, ans, 1);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.size = buf_size;

    CEPH_BUFF_ADD(ans, list, buf_size);

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LISTXATTR, req, ans);
    if (err >= 0) {
        *list_size = ans.size;
    }

    return err;
}

__public int
ceph_ll_lookup(struct ceph_mount_info *cmount, Inode *parent, const char *name,
               Inode **out, struct ceph_statx *stx, unsigned want,
               unsigned flags, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_lookup, req, 1, ans, 1);
    ceph_dentry_t *dentry;
    int32_t err;

    if ((*name == '.') && (name[1] == 0)) {
        *out = inode_ref(parent);
        memcpy(stx, &parent->stx, sizeof(*stx));

        return 0;
    }

    dentry = dentry_lookup(parent, name);
    if (dentry != NULL) {
        *out = inode_ref(dentry->inode);
        memcpy(stx, &dentry->inode->stx, sizeof(*stx));

        return 0;
    }

    req.userperm = ptr_value(perms);
    req.parent = parent->inode;
    req.want = want;
    req.flags = flags;
    CEPH_STR_ADD(req, name, name);

    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, out, ans.inode, stx);
        if (err >= 0) {
            err = dentry_create(cmount, parent, *out, name);
        }
        if (err >= 0) {
            if ((*name == '.') && (name[1] == 0)) {
                cmount->cwd_inode = inode_ref(*out);
            }
            if ((cmount->root_inode == NULL) &&
                (stx->stx_ino == CEPH_INO_ROOT)) {
                cmount->root_inode = inode_ref(*out);
            }
        }
    }

    return err;
}

__public int
ceph_ll_lookup_inode(struct ceph_mount_info *cmount, struct inodeno_t ino,
                     Inode **inode)
{
    CEPH_REQ(ceph_ll_lookup_inode, req, 0, ans, 0);
    int32_t err;

    if ((ino.val == CEPH_INO_ROOT) && (cmount->root_inode != NULL)) {
        *inode = inode_ref(cmount->root_inode);
        return 0;
    }
    if ((cmount->cwd_inode != NULL) &&
        (cmount->cwd_inode->stx.stx_ino == ino.val)) {
        *inode = inode_ref(cmount->cwd_inode);
        return 0;
    }
    *inode = inode_lookup(ino.val);
    if (*inode != NULL) {
        return 0;
    }

    req.ino = ino;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_INODE, req, ans);
    if (err >= 0) {
        err = inode_create_ino(cmount, inode, ans.inode, ino.val);
        if ((err >= 0) && (ino.val == CEPH_INO_ROOT)) {
            cmount->root_inode = inode_ref(*inode);
            if ((cmount->cwd_inode == NULL) && (*cmount->cwd == '/') &&
                (cmount->cwd[1] == 0)) {
                cmount->cwd_inode = inode_ref(cmount->root_inode);
            }
        }
    }

    return err;
}

__public int
ceph_ll_lookup_root(struct ceph_mount_info *cmount, Inode **parent)
{
    CEPH_REQ(ceph_ll_lookup_root, req, 0, ans, 0);
    int32_t err;

    if (cmount->root_inode != NULL) {
        *parent = inode_ref(cmount->root_inode);
        return 0;
    }

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_ROOT, req, ans);
    if (err >= 0) {
        err = inode_create_ino(cmount, &cmount->root_inode, ans.inode,
                               CEPH_INO_ROOT);
        if (err >= 0) {
            *parent = inode_ref(cmount->root_inode);
            if ((cmount->cwd_inode == NULL) && (*cmount->cwd == '/') &&
                (cmount->cwd[1] == 0)) {
                cmount->cwd_inode = inode_ref(cmount->root_inode);
            }
        }
    }

    return err;
}

__public off_t
ceph_ll_lseek(struct ceph_mount_info *cmount, struct Fh *filehandle,
              off_t offset, int whence)
{
    CEPH_REQ(ceph_ll_lseek, req, 0, ans, 0);
    int32_t err;

    req.fh = ptr_value(filehandle);
    req.offset = offset;
    req.whence = whence;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LSEEK, req, ans);
    if (err >= 0) {
        return ans.offset;
    }

    return err;
}

__public int
ceph_ll_mkdir(struct ceph_mount_info *cmount, Inode *parent, const char *name,
              mode_t mode, Inode **out, struct ceph_statx *stx, unsigned want,
              unsigned flags, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_mkdir, req, 1, ans, 1);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.parent = parent->inode;
    req.mode = mode;
    req.want = want | CEPH_STATX_INO;
    req.flags = flags;
    CEPH_STR_ADD(req, name, name);

    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKDIR, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, out, ans.inode, stx);
        if (err >= 0) {
            err = dentry_create(cmount, parent, *out, name);
        }
    }

    return err;
}

__public int
ceph_ll_mknod(struct ceph_mount_info *cmount, Inode *parent, const char *name,
              mode_t mode, dev_t rdev, Inode **out, struct ceph_statx *stx,
              unsigned want, unsigned flags, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_mknod, req, 1, ans, 1);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.parent = parent->inode;
    req.mode = mode;
    req.rdev = rdev;
    req.want = want | CEPH_STATX_INO;
    req.flags = flags;
    CEPH_STR_ADD(req, name, name);

    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKNOD, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, out, ans.inode, stx);
        if (err >= 0) {
            err = dentry_create(cmount, parent, *out, name);
        }
    }

    return err;
}

__public int
ceph_ll_open(struct ceph_mount_info *cmount, struct Inode *in, int flags,
             struct Fh **fh, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_open, req, 0, ans, 0);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.flags = flags;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPEN, req, ans);
    if (err >= 0) {
        *fh = value_ptr(ans.fh);
    }

    return err;
}

__public int
ceph_ll_opendir(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_dir_result **dirpp, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_opendir, req, 0, ans, 0);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.inode = in->inode;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPENDIR, req, ans);
    if (err >= 0) {
        *dirpp = value_ptr(ans.dir);
    }

    return err;
}

__public int
ceph_ll_put(struct ceph_mount_info *cmount, struct Inode *in)
{
    CEPH_REQ(ceph_ll_put, req, 0, ans, 0);
    int32_t err;

    if (!inode_unref(in)) {
        return 0;
    }

    req.inode = in->inode;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_PUT, req, ans);
    if (err < 0) {
        inode_ref(in);
    } else {
        inode_destroy(in);
    }

    return err;
}

__public int
ceph_ll_read(struct ceph_mount_info *cmount, struct Fh *filehandle, int64_t off,
             uint64_t len, char *buf)
{
    CEPH_REQ(ceph_ll_read, req, 0, ans, 1);

    req.fh = ptr_value(filehandle);
    req.offset = off;
    req.len = len;

    CEPH_BUFF_ADD(ans, buf, len);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READ, req, ans);
}

__public int
ceph_ll_readlink(struct ceph_mount_info *cmount, struct Inode *in, char *buf,
                 size_t bufsize, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_readlink, req, 0, ans, 1);

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.size = bufsize;

    CEPH_BUFF_ADD(ans, buf, bufsize);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READLINK, req, ans);
}

__public int
ceph_ll_releasedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dir)
{
    CEPH_REQ(ceph_ll_releasedir, req, 0, ans, 0);

    req.dir = ptr_value(dir);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RELEASEDIR, req, ans);
}

__public int
ceph_ll_removexattr(struct ceph_mount_info *cmount, struct Inode *in,
                    const char *name, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_removexattr, req, 1, ans, 0);

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    CEPH_STR_ADD(req, name, name);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_REMOVEXATTR, req, ans);
}

__public int
ceph_ll_rename(struct ceph_mount_info *cmount, struct Inode *parent,
               const char *name, struct Inode *newparent, const char *newname,
               const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_rename, req, 2, ans, 0);

    req.userperm = ptr_value(perms);
    req.old_parent = parent->inode;
    req.new_parent = newparent->inode;
    CEPH_STR_ADD(req, old_name, name);
    CEPH_STR_ADD(req, new_name, newname);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RENAME, req, ans);
}

__public void
ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
    CEPH_REQ(ceph_rewinddir, req, 0, ans, 0);

    req.dir = ptr_value(dirp);

    CEPH_PROCESS(cmount, LIBCEPHFSD_OP_REWINDDIR, req, ans);
}

__public int
ceph_ll_rmdir(struct ceph_mount_info *cmount, struct Inode *in,
              const char *name, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_rmdir, req, 1, ans, 0);

    req.userperm = ptr_value(perms);
    req.parent = in->inode;
    CEPH_STR_ADD(req, name, name);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RMDIR, req, ans);
}

__public int
ceph_ll_setattr(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_statx *stx, int mask, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_setattr, req, 1, ans, 0);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.mask = mask;
    CEPH_BUFF_ADD(req, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETATTR, req, ans);
    if (err >= 0) {
        inode_update(in, stx);
    }

    return err;
}

__public int
ceph_ll_setxattr(struct ceph_mount_info *cmount, struct Inode *in,
                 const char *name, const void *value, size_t size, int flags,
                 const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_setxattr, req, 2, ans, 0);

    req.userperm = ptr_value(perms);
    req.inode = in->inode;
    req.size = size;
    req.flags = flags;
    CEPH_STR_ADD(req, name, name);
    CEPH_BUFF_ADD(req, value, size);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETXATTR, req, ans);
}

__public int
ceph_ll_statfs(struct ceph_mount_info *cmount, struct Inode *in,
               struct statvfs *stbuf)
{
    CEPH_REQ(ceph_ll_statfs, req, 0, ans, 1);

    req.inode = in->inode;

    CEPH_BUFF_ADD(ans, stbuf, sizeof(*stbuf));

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_STATFS, req, ans);
}

__public int
ceph_ll_symlink(struct ceph_mount_info *cmount, Inode *in, const char *name,
                const char *value, Inode **out, struct ceph_statx *stx,
                unsigned want, unsigned flags, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_symlink, req, 2, ans, 1);
    int32_t err;

    req.userperm = ptr_value(perms);
    req.parent = in->inode;
    req.want = want | CEPH_STATX_INO;
    req.flags = flags;
    CEPH_STR_ADD(req, name, name);
    CEPH_STR_ADD(req, target, value);

    CEPH_BUFF_ADD(req, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SYMLINK, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, out, ans.inode, stx);
        if (err >= 0) {
            err = dentry_create(cmount, in, *out, name);
        }
    }

    return err;
}

__public int
ceph_ll_unlink(struct ceph_mount_info *cmount, struct Inode *in,
               const char *name, const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_unlink, req, 1, ans, 0);

    req.userperm = ptr_value(perms);
    req.parent = in->inode;
    CEPH_STR_ADD(req, name, name);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_UNLINK, req, ans);
}

__public int
ceph_ll_walk(struct ceph_mount_info *cmount, const char *name, Inode **i,
             struct ceph_statx *stx, unsigned int want, unsigned int flags,
             const UserPerm *perms)
{
    CEPH_REQ(ceph_ll_walk, req, 1, ans, 1);
    int32_t err;

    if (cmount->cwd_inode != NULL) {
        if ((strcmp(cmount->cwd, name) == 0) || ((*name == '.') &&
            ((name[1] == 0) || ((name[1] == '/') && (name[2] == 0))))) {
            /* TODO: don't ignore perms. */
            *i = inode_ref(cmount->cwd_inode);
            memcpy(stx, &cmount->cwd_inode->stx, sizeof(*stx));
            return 0;
        }
    }
    if ((cmount->root_inode != NULL) && (*name == '/') && (name[1] == 0)) {
        /* TODO: don't ignore perms. */
        *i = inode_ref(cmount->root_inode);
        memcpy(stx, &cmount->root_inode->stx, sizeof(*stx));
        return 0;
    }

    req.userperm = ptr_value(perms);
    req.want = want | CEPH_STATX_INO;
    req.flags = flags;
    CEPH_STR_ADD(req, path, name);

    CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WALK, req, ans);
    if (err >= 0) {
        err = inode_create(cmount, i, ans.inode, stx);
        if (err >= 0) {
            if ((*name == '.') &&
                ((name[1] == 0) || ((name[1] == '/') && (name[2] == 0)))) {
                cmount->cwd_inode = inode_ref(*i);
            }
            if ((*name == '/') && (name[1] == 0)) {
                cmount->root_inode = inode_ref(*i);
            }
        }
    }

    return err;
}

__public int
ceph_ll_write(struct ceph_mount_info *cmount, struct Fh *filehandle,
              int64_t off, uint64_t len, const char *data)
{
    CEPH_REQ(ceph_ll_write, req, 1, ans, 0);

    req.fh = ptr_value(filehandle);
    req.offset = off;
    req.len = len;
    CEPH_BUFF_ADD(req, data, len);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WRITE, req, ans);
}

__public int
ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
    CEPH_REQ(ceph_mount, req, 1, ans, 0);

    CEPH_STR_ADD(req, root, root);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_MOUNT, req, ans);
}

__public struct dirent *
ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
    static struct dirent de;
    int32_t err;

    CEPH_REQ(ceph_readdir, req, 0, ans, 1);

    req.dir = ptr_value(dirp);

    CEPH_BUFF_ADD(ans, &de, sizeof(de));

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_READDIR, req, ans);
    if (err >= 0) {
        return &de;
    }

    errno = -err;

    return NULL;
}

__public int
ceph_release(struct ceph_mount_info *cmount)
{
    CEPH_REQ(ceph_release, req, 0, ans, 0);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_RELEASE, req, ans);
}

__public int
ceph_select_filesystem(struct ceph_mount_info *cmount, const char *fs_name)
{
    CEPH_REQ(ceph_select_filesystem, req, 1, ans, 0);

    CEPH_STR_ADD(req, fs, fs_name);

    return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_SELECT_FILESYSTEM, req, ans);
}

__public int
ceph_unmount(struct ceph_mount_info *cmount)
{
    CEPH_REQ(ceph_unmount, req, 0, ans, 0);
    int32_t err;

    req.root_inode = cmount->root_inode == NULL ? 0 : cmount->root_inode->inode;
    req.cwd_inode = cmount->cwd_inode == NULL ? 0 : cmount->cwd_inode->inode;

    err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_UNMOUNT, req, ans);
    if (err >= 0) {
        *cmount->cwd = 0;
        if (inode_unref(cmount->root_inode)) {
            inode_destroy(cmount->root_inode);
        }
        cmount->root_inode = NULL;
        if (inode_unref(cmount->cwd_inode)) {
            inode_destroy(cmount->cwd_inode);
        }
        cmount->cwd_inode = NULL;
    }

    return err;
}

__public void
ceph_userperm_destroy(UserPerm *perms)
{
    CEPH_REQ(ceph_userperm_destroy, req, 0, ans, 0);

    req.userperm = ptr_value(perms);

    CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_DESTROY, req, ans);
}

__public UserPerm *
ceph_userperm_new(uid_t uid, gid_t gid, int ngids, gid_t *gidlist)
{
    CEPH_REQ(ceph_userperm_new, req, 1, ans, 0);
    int32_t err;

    req.uid = uid;
    req.gid = gid;
    req.groups = ngids;
    CEPH_BUFF_ADD(req, gidlist, sizeof(gid_t) * ngids);

    err = proxy_global_connect();
    if (err >= 0) {
        err = CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_NEW, req, ans);
    }
    if (err >= 0) {
        return value_ptr(ans.userperm);
    }

    errno = -err;

    return NULL;
}

__public const char *
ceph_version(int *major, int *minor, int *patch)
{
    static char cached_version[128];
    static int32_t cached_major = -1, cached_minor, cached_patch;

    if (cached_major < 0) {
        CEPH_REQ(ceph_version, req, 0, ans, 1);
        int32_t err;

        CEPH_BUFF_ADD(ans, cached_version, sizeof(cached_version));

        err = proxy_global_connect();
        if (err >= 0) {
            err = CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_VERSION, req, ans);
        }

        if (err < 0) {
            *major = 0;
            *minor = 0;
            *patch = 0;

            return "Unknown";
        }

        cached_major = ans.major;
        cached_minor = ans.minor;
        cached_patch = ans.patch;
    }

    *major = cached_major;
    *minor = cached_minor;
    *patch = cached_patch;

    return cached_version;
}
