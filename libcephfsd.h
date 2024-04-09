
#ifndef __LIBCEPHFSD_H__
#define __LIBCEPHFSD_H__

#include <stdint.h>
#include <unistd.h>

struct ceph_mount_info;
typedef struct _UserPerm UserPerm;
typedef struct _Inode Inode;
struct Fh;
typedef struct Fh Fh;

int32_t
ceph_chdir(struct ceph_mount_info *cmount, const char *path);

int32_t
ceph_conf_get(struct ceph_mount_info *cmount, const char *option, char *buf,
              size_t len);

int32_t
ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path_list);

int32_t
ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
              const char *value);

int32_t
ceph_create(struct ceph_mount_info **cmount, const char * const id);

const char *
ceph_getcwd(struct ceph_mount_info *cmount);

int32_t
ceph_init(struct ceph_mount_info *cmount);

int32_t
ceph_ll_close(struct ceph_mount_info *cmount, struct Fh* filehandle);

int32_t
ceph_ll_create(struct ceph_mount_info *cmount, Inode *parent, const char *name,
               mode_t mode, int oflags, Inode **outp, Fh **fhp,
               struct ceph_statx *stx, unsigned want, unsigned lflags,
               const UserPerm *perms);

int32_t
ceph_ll_fallocate(struct ceph_mount_info *cmount, struct Fh *fh, int mode,
                  int64_t offset, int64_t length);

int32_t
ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh, int syncdataonly);

int32_t
ceph_ll_getattr(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_statx *stx, unsigned int want, unsigned int flags,
                const UserPerm *perms);

int32_t
ceph_ll_getxattr(struct ceph_mount_info *cmount, struct Inode *in,
                 const char *name, void *value, size_t size,
                 const UserPerm *perms);

int32_t
ceph_ll_link(struct ceph_mount_info *cmount, struct Inode *in,
             struct Inode *newparent, const char *name, const UserPerm *perms);

int32_t
ceph_ll_listxattr(struct ceph_mount_info *cmount, struct Inode *in, char *list,
                  size_t buf_size, size_t *list_size, const UserPerm *perms);

int32_t
ceph_ll_lookup(struct ceph_mount_info *cmount, Inode *parent, const char *name,
               Inode **out, struct ceph_statx *stx, unsigned want,
               unsigned flags, const UserPerm *perms);

int32_t
ceph_ll_lookup_inode(struct ceph_mount_info *cmount, struct inodeno_t ino,
                     Inode **inode);

int32_t
ceph_ll_lookup_root(struct ceph_mount_info *cmount, Inode **parent);

off_t
ceph_ll_lseek(struct ceph_mount_info *cmount, struct Fh* filehandle,
              off_t offset, int whence);

int32_t
ceph_ll_mkdir(struct ceph_mount_info *cmount, Inode *parent, const char *name,
              mode_t mode, Inode **out, struct ceph_statx *stx, unsigned want,
              unsigned flags, const UserPerm *perms);

int32_t
ceph_ll_mknod(struct ceph_mount_info *cmount, Inode *parent, const char *name,
              mode_t mode, dev_t rdev, Inode **out, struct ceph_statx *stx,
              unsigned want, unsigned flags, const UserPerm *perms);

int32_t
ceph_ll_open(struct ceph_mount_info *cmount, struct Inode *in, int flags,
             struct Fh **fh, const UserPerm *perms);

int32_t
ceph_ll_opendir(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_dir_result **dirpp, const UserPerm *perms);

int32_t
ceph_ll_read(struct ceph_mount_info *cmount, struct Fh* filehandle, int64_t off,
             uint64_t len, char* buf);

int32_t
ceph_ll_readlink(struct ceph_mount_info *cmount, struct Inode *in, char *buf,
                 size_t bufsize, const UserPerm *perms);

int32_t
ceph_ll_releasedir(struct ceph_mount_info *cmount, struct ceph_dir_result* dir);

int32_t
ceph_ll_removexattr(struct ceph_mount_info *cmount, struct Inode *in,
                    const char *name, const UserPerm *perms);

int32_t
ceph_ll_rename(struct ceph_mount_info *cmount, struct Inode *parent,
               const char *name, struct Inode *newparent, const char *newname,
               const UserPerm *perms);

int32_t
ceph_ll_rmdir(struct ceph_mount_info *cmount, struct Inode *in,
              const char *name, const UserPerm *perms);

int32_t
ceph_ll_setattr(struct ceph_mount_info *cmount, struct Inode *in,
                struct ceph_statx *stx, int mask, const UserPerm *perms);

int32_t
ceph_ll_setxattr(struct ceph_mount_info *cmount, struct Inode *in,
                 const char *name, const void *value, size_t size, int flags,
                 const UserPerm *perms);

int32_t
ceph_ll_statfs(struct ceph_mount_info *cmount, struct Inode *in,
               struct statvfs *stbuf);

int32_t
ceph_ll_symlink(struct ceph_mount_info *cmount, Inode *in, const char *name,
                const char *value, Inode **out, struct ceph_statx *stx,
                unsigned want, unsigned flags, const UserPerm *perms);

int32_t
ceph_ll_unlink(struct ceph_mount_info *cmount, struct Inode *in,
               const char *name, const UserPerm *perms);

int32_t
ceph_ll_walk(struct ceph_mount_info *cmount, const char* name, Inode **i,
             struct ceph_statx *stx, unsigned int want, unsigned int flags,
             const UserPerm *perms);

int32_t
ceph_ll_write(struct ceph_mount_info *cmount, struct Fh* filehandle,
              int64_t off, uint64_t len, const char *data);

int32_t
ceph_mount(struct ceph_mount_info *cmount, const char *root);

struct dirent *
ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);

int32_t
ceph_release(struct ceph_mount_info *cmount);

int32_t
ceph_select_filesystem(struct ceph_mount_info *cmount, const char *fs_name);

int32_t
ceph_unmount(struct ceph_mount_info *cmount);

UserPerm *
ceph_userperm_new(uid_t uid, gid_t gid, int ngids, gid_t *gidlist);

#endif
