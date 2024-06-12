
#include <stdio.h>

#include "proxy_log.h"

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

static char data[4096];

static void
log_write(proxy_log_handler_t *handler, int32_t level, int32_t err,
          const char *msg)
{
    printf("[%d] %s\n", level, msg);
}

static void
show_statx(const char *text, struct ceph_statx *stx)
{
    printf("%s:\n", text);
    printf("     mask:");
    if ((stx->stx_mask & CEPH_STATX_MODE) != 0) {
        printf(" mode");
    }
    if ((stx->stx_mask & CEPH_STATX_NLINK) != 0) {
        printf(" nlink");
    }
    if ((stx->stx_mask & CEPH_STATX_UID) != 0) {
        printf(" uid");
    }
    if ((stx->stx_mask & CEPH_STATX_GID) != 0) {
        printf(" gid");
    }
    if ((stx->stx_mask & CEPH_STATX_RDEV) != 0) {
        printf(" rdev");
    }
    if ((stx->stx_mask & CEPH_STATX_ATIME) != 0) {
        printf(" atime");
    }
    if ((stx->stx_mask & CEPH_STATX_MTIME) != 0) {
        printf(" mtime");
    }
    if ((stx->stx_mask & CEPH_STATX_CTIME) != 0) {
        printf(" ctime");
    }
    if ((stx->stx_mask & CEPH_STATX_INO) != 0) {
        printf(" ino");
    }
    if ((stx->stx_mask & CEPH_STATX_SIZE) != 0) {
        printf(" size");
    }
    if ((stx->stx_mask & CEPH_STATX_BLOCKS) != 0) {
        printf(" blocks");
    }
    if ((stx->stx_mask & CEPH_STATX_BTIME) != 0) {
        printf(" btime");
    }
    if ((stx->stx_mask & CEPH_STATX_VERSION) != 0) {
        printf(" version");
    }
    printf("\n");
    printf("  blksize: %u\n", stx->stx_blksize);
    printf("    nlink: %u\n", stx->stx_nlink);
    printf("      uid: %u\n", stx->stx_uid);
    printf("      gid: %u\n", stx->stx_gid);
    printf("     mode: %o\n", stx->stx_mode);
    printf("      ino: %lu\n", stx->stx_ino);
    printf("     size: %lu\n", stx->stx_size);
    printf("   blocks: %lu\n", stx->stx_blocks);
    printf("      dev: %lx\n", stx->stx_dev);
    printf("     rdev: %lx\n", stx->stx_rdev);
    printf("    atime: %lu.%09lu\n", stx->stx_atime.tv_sec, stx->stx_atime.tv_nsec);
    printf("    ctime: %lu.%09lu\n", stx->stx_ctime.tv_sec, stx->stx_ctime.tv_nsec);
    printf("    mtime: %lu.%09lu\n", stx->stx_mtime.tv_sec, stx->stx_mtime.tv_nsec);
    printf("    btime: %lu.%09lu\n", stx->stx_btime.tv_sec, stx->stx_btime.tv_nsec);
    printf("  version: %lu\n", stx->stx_version);
}

int32_t
main(int32_t argc, char *argv[])
{
    proxy_log_handler_t log_handler;
    struct ceph_statx stx;
    const char *text;
    struct ceph_mount_info *cmount;
    UserPerm *perms;
    struct Inode *root, *dir, *file;
    struct Fh *fh;
    int32_t err, major, minor, patch;

    if (argc < 3) {
        printf("Usage: libcephfsd_test <id> <config file> [<fs>]\n");
        return 1;
    }

    proxy_log_register(&log_handler, log_write);

    text = ceph_version(&major, &minor, &patch);
    printf("%d.%d.%d (%s)\n", major, minor, patch, text);

    err = 0;
    CHECK(err, ceph_create, &cmount, argv[1]);
    CHECK(err, ceph_conf_read_file, cmount, argv[2]);
    CHECK(err, ceph_conf_get, cmount, "log file", data, sizeof(data));
    CHECK(err, ceph_conf_set, cmount, "client_acl_type", "posix_acl");
    CHECK(err, ceph_conf_get, cmount, "client_acl_type", data, sizeof(data));
    CHECK(err, ceph_conf_set, cmount, "fuse_default_permissions", "false");
    CHECK(err, ceph_init, cmount);
    CHECK(err, ceph_select_filesystem, cmount, "sit_fs");
    CHECK(err, ceph_mount, cmount, NULL);
    perms = CHECK_PTR(err, ceph_userperm_new, 0, 0, 0, NULL);
    CHECK(err, ceph_ll_lookup_root, cmount, &root);
    CHECK(err, ceph_ll_mkdir, cmount, root, "dir.1", 0755, &dir, &stx,
                              CEPH_STATX_INO, 0, perms);
    if (err >= 0) {
        show_statx("dir.1", &stx);
    }
    CHECK(err, ceph_ll_create, cmount, dir, "file.1", 0644,
                               O_CREAT | O_TRUNC | O_RDWR, &file, &fh, &stx, 0,
                               0, perms);
    if (err >= 0) {
        show_statx("file.1", &stx);
    }
    CHECK(err, ceph_ll_write, cmount, fh, 0, sizeof(stx), (char *)&stx);
    memset(&stx, 0, sizeof(stx));
    CHECK(err, ceph_ll_read, cmount, fh, 0, sizeof(stx), (char *)&stx);
    if (err >= 0) {
        show_statx("file.1", &stx);
    }
    CHECK(err, ceph_ll_close, cmount, fh);
    CHECK(err, ceph_ll_unlink, cmount, dir, "file.1", perms);
    CHECK(err, ceph_ll_rmdir, cmount, root, "dir.1", perms);
    CHECK(err, ceph_unmount, cmount);
    CHECK(err, ceph_release, cmount);

    proxy_log_deregister(&log_handler);

    return err < 0 ? 1 : 0;
}
