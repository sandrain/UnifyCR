#ifndef __UNIFYFS_STORAGE_H
#define __UNIFYFS_STORAGE_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int unifyfs_storage_init(const char *root, const char *mountpoint);

int unifyfs_storage_open(const char *pathname, int flags, mode_t mode);

int unifyfs_storage_close(int fd);

int unifyfs_storage_stat(const char *path, struct stat *buf);

off_t unifyfs_storage_lseek(int fd, off_t offset, int whence);

ssize_t unifyfs_storage_write(int fd, const void *buf, size_t count);

ssize_t unifyfs_storage_read(int fd, void *buf, size_t count);

int unifyfs_storage_fsync(int fd);

int unifyfs_storage_fdatasync(int fd);

int unifyfs_storage_mkdir(const char *path, mode_t mode);

#endif /* __UNIFYFS_STORAGE_H */
