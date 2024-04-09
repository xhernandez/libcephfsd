# Proxy daemon for libcephfs

This is a Work In Progress

## Build

Just run `make`. It will create 3 components:

* _libcephfs_proxy.so_
  This is a library that mimics libcephfs exported functions and forwards all
  requests to the libcephfsd daemon.

* _libcephfsd_
  This is the daemon that will receive requests from the proxy library and
  execute them using the real libcephfs library.

* _libcephfsd_test_
  This is a test program.

Running `make install` will copy the libcephfs_proxy library to /usr/lib64.

You need to compile vfs_ceph_ll module against libcephfs_proxy.so instead of
libcephfs.so.

## Execute

In one terminal session, run libcephfsd. For now it runs in the foreground.
Then start smbd and connect clients normally.
