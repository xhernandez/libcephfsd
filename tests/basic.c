
#include "test_common.h"

static char data[4096];

int32_t
main(int32_t argc, char *argv[])
{
    struct ceph_statx stx;
    struct ceph_mount_info *cmount;
    UserPerm *perms;
    struct Inode *root, *dir, *file;
    struct Fh *fh;
    int32_t err;

    if (argc < 3) {
        printf("Usage: %s <id> <config file> [<fs>]\n", argv[0]);
        return 1;
    }

    test_init();

    err = 0;
    CHECK(err, ceph_create, &cmount, argv[1]);
    CHECK(err, ceph_conf_read_file, cmount, argv[2]);
    CHECK(err, ceph_conf_get, cmount, "log file", data, sizeof(data));
    CHECK(err, ceph_conf_set, cmount, "client_acl_type", "posix_acl");
    CHECK(err, ceph_conf_get, cmount, "client_acl_type", data, sizeof(data));
    CHECK(err, ceph_conf_set, cmount, "fuse_default_permissions", "false");
    CHECK(err, ceph_init, cmount);
    if (argc > 3) {
        CHECK(err, ceph_select_filesystem, cmount, argv[3]);
    }
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

    test_done();

    return err < 0 ? 1 : 0;
}
