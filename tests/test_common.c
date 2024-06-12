
#include "test_common.h"

static proxy_log_handler_t log_handler;

static void
log_write(proxy_log_handler_t *handler, int32_t level, int32_t err,
          const char *msg)
{
    printf("[%d] %s\n", level, msg);
}

void
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

void
test_init(void)
{
    const char *text;
    int32_t major, minor, patch;

    proxy_log_register(&log_handler, log_write);

    text = ceph_version(&major, &minor, &patch);
    printf("Ceph version: %d.%d.%d (%s)\n", major, minor, patch, text);
}

void
test_done(void)
{
    proxy_log_deregister(&log_handler);
}
