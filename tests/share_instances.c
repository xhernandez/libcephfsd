
#include "test_common.h"

int32_t
main(int32_t argc, char *argv[])
{
    struct ceph_mount_info *cmount1, *cmount2, *cmount3;
    int32_t err;

    if (argc < 3) {
        printf("Usage: %s <id> <config file> [<fs>]\n", argv[0]);
        return 1;
    }

    test_init();

    err = 0;
    CHECK(err, ceph_create, &cmount1, argv[1]);
    CHECK(err, ceph_create, &cmount2, argv[1]);
    CHECK(err, ceph_create, &cmount3, argv[1]);

    CHECK(err, ceph_conf_read_file, cmount1, argv[2]);
    CHECK(err, ceph_conf_read_file, cmount2, argv[2]);
    CHECK(err, ceph_conf_read_file, cmount3, argv[2]);

    CHECK(err, ceph_conf_set, cmount1, "client_acl_type", "posix_acl");
    CHECK(err, ceph_conf_set, cmount2, "client_acl_type", "posix_acl");
    CHECK(err, ceph_conf_set, cmount3, "client_acl_type", "posix_acl");

    CHECK(err, ceph_conf_set, cmount1, "bdev_enable_discard", "true");
    CHECK(err, ceph_conf_set, cmount2, "bdev_enable_discard", "true");

    CHECK(err, ceph_init, cmount1);
    CHECK(err, ceph_init, cmount2);
    CHECK(err, ceph_init, cmount3);

    CHECK(err, ceph_mount, cmount1, NULL);
    CHECK(err, ceph_mount, cmount2, NULL);
    CHECK(err, ceph_mount, cmount3, NULL);

    CHECK(err, ceph_unmount, cmount2);

    CHECK(err, ceph_mount, cmount2, NULL);

    CHECK(err, ceph_unmount, cmount3);
    CHECK(err, ceph_conf_set, cmount3, "bdev_enable_discard", "true");
    CHECK(err, ceph_mount, cmount3, NULL);

    CHECK(err, ceph_unmount, cmount1);
    CHECK(err, ceph_conf_set, cmount1, "bdev_enable_discard", "false");
    CHECK(err, ceph_mount, cmount1, NULL);

    CHECK(err, ceph_unmount, cmount1);
    CHECK(err, ceph_unmount, cmount2);
    CHECK(err, ceph_unmount, cmount3);

    CHECK(err, ceph_release, cmount1);
    CHECK(err, ceph_release, cmount2);
    CHECK(err, ceph_release, cmount3);

    test_done();

    return err < 0 ? 1 : 0;
}
