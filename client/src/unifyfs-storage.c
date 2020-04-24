/*
 * Copyright (c) 2019, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 *
 * Copyright 2019, UT-Battelle, LLC.
 *
 * LLNL-CODE-741539
 * All rights reserved.
 *
 * This is the license for UnifyFS.
 * For details, see https://github.com/LLNL/UnifyFS.
 * Please read https://github.com/LLNL/UnifyFS/LICENSE for full license text.
 */

#include "unifyfs-internal.h"
#include "unifyfs-storage.h"
#include "unifyfs_log.h"
#include "margo_client.h"
#include "unifyfs_fmap.h"

#include <pthread.h>

extern int client_rank;
extern int local_rank_cnt;

static const char *unifyfs_conroot;
static const char *unifyfs_mountpoint;

struct unifyfs_fd {
    int flags;
    struct unifyfs_fmap *fmap;
};

#define UNIFYFS_STORAGE_FD_MAX 1024

static struct unifyfs_fd _ufd[UNIFYFS_STORAGE_FD_MAX];

static inline void unifyfs_fd_set_flags(int fd, int flags)
{
    _ufd[fd].flags = flags;
}

static inline int unifyfs_fd_get_flags(int fd)
{
    return _ufd[fd].flags;
}

static inline void unifyfs_fd_set_fmap(int fd, struct unifyfs_fmap *fmap)
{
    LOGDBG("setting fmap for fd=%d", fd);

#ifdef UNIFYFS_FMAP_DEBUG
    unifyfs_fmap_print(fmap);
#endif

    _ufd[fd].fmap = fmap;
}

static inline struct unifyfs_fmap *unifyfs_fd_get_fmap(int fd)
{
    return _ufd[fd].fmap;
}

static inline char *__conpath(char *conpath, const char *pathname)
{
    sprintf(conpath, "%s%s", unifyfs_conroot, pathname);

    return conpath;
}

#if 0
static int pathname_from_fd(int fd, char *pathbuf)
{
    int ret = 0;
    char procpath[64] = { 0, };
    char path[PATH_MAX] = { 0, };
    size_t len = 0;

    if (!pathbuf)
        return EINVAL;

    sprintf(procpath, "/proc/self/fd/%d", fd);

    ret = readlink(procpath, path, PATH_MAX-1);
    if (ret < 0) {
        LOGDBG("failed to retreive the path from fd (%d)", fd);
        return errno;
    }

    len = strlen(unifyfs_conroot);

    if (0 != strncmp(unifyfs_conroot, path, len)) {
        /* this is not our file? */
        return EINVAL;
    }

    strcpy(pathbuf, &path[len]);

    return 0;
}
#endif

int unifyfs_storage_init(const char *root, const char *mountpoint)
{
    int ret = 0;
    struct stat sb = { 0, };
    char conpath[PATH_MAX] = { 0, };

    if (!root) {
        return EINVAL;
    }

    ret = UNIFYFS_REAL(stat)(root, &sb);
    if (ret) {
        LOGERR("%s is not a valid path (%s)", root, strerror(errno));
        return errno;
    }

    unifyfs_conroot = realpath(root, NULL);
    if (!unifyfs_conroot) {
        LOGERR("%s", strerror(errno));
        return errno;
    }

    ret = UNIFYFS_REAL(stat)(__conpath(conpath, mountpoint), &sb);
    if (0 == ret && S_ISDIR(sb.st_mode)) {
        ret = 0;
    } else if (ENOENT == errno) {
        ret = unifyfs_storage_mkdir(mountpoint, 0755);
    } else {
        ret = errno;
    }

    unifyfs_mountpoint = mountpoint;

    return ret;
}

static int storage_map_fmap(int fd)
{
    int ret = 0;
    int shmfd = 0;
    struct stat sb = { 0, };
    char fmap_name[PATH_MAX] = { 0, };
    size_t len = 0;
    void *addr = NULL;

    ret = fstat(fd, &sb);
    if (ret < 0) {
        LOGERR("failed to stat: %s", strerror(errno));
        ret = errno;
        goto out;
    }

    unifyfs_fmap_shm_name(fmap_name, sb.st_ino);

    shmfd = shm_open(fmap_name, O_RDONLY, 0644);
    if (shmfd < 0) {
        LOGERR("failed to open shm (%s): %s", fmap_name, strerror(errno));
        goto out;
    }

    ret = fstat(fd, &sb);
    if (ret < 0) {
        LOGERR("failed to stat: %s", strerror(errno));
        ret = errno;
        goto out;
    }

    len = sb.st_size;

    addr = mmap(NULL, len, PROT_READ, MAP_SHARED, shmfd, 0);
    if (MAP_FAILED == addr) {
        LOGERR("failed to mmap the fmap (%s)", strerror(errno));
        goto out;
    }

    unifyfs_fd_set_fmap(fd, (struct unifyfs_fmap *) addr);

out:
    return ret;
}

int unifyfs_storage_open(const char *pathname, int flags, mode_t mode)
{
    int ret = 0;
    int fd = 0;
    char conpath[PATH_MAX] = { 0, };

    ret = unifyfs_invoke_lsm_open(pathname, flags, mode);
    if (ret) {
        LOGERR("lsm_open failed (ret=%d)", ret);
        goto out_fail;
    }

    fd = UNIFYFS_REAL(open)(__conpath(conpath, pathname), flags, mode);
    if (fd < 0) {
        LOGERR("open failed: %s (%d: %s)", pathname, errno, strerror(errno));
        ret = errno;
        goto out_fail;
    }

    if (flags == O_RDONLY) {
        ret = storage_map_fmap(fd);
        if (ret < 0) {
            LOGERR("failed to map the fmap (ret=%d)", ret);
            close(fd);
            goto out_fail;
        }
    }

    unifyfs_fd_set_flags(fd, flags);

    return fd;

out_fail:
    errno = ret;
    return -1;
}

int unifyfs_storage_close(int fd)
{
    int ret = 0;
    int flags = 0;
    struct stat sb = { 0, };

    ret = UNIFYFS_REAL(fstat)(fd, &sb);
    if (ret) {
        LOGERR("stat failed (%s)", strerror(errno));
        goto out;
    }

    UNIFYFS_REAL(close)(fd);

    flags = unifyfs_fd_get_flags(fd);
    if (O_RDONLY == flags) {
        goto out;   /* no need to inform server */
    }

    ret = unifyfs_invoke_lsm_close(sb.st_ino);
    if (ret) {
        LOGERR("lsm_close failed (ret=%d)", ret);
    }

out:
    return ret;
}

int unifyfs_storage_stat(const char *path, struct stat *buf)
{
    int ret = 0;
    struct stat sb = { 0, };
    char conpath[PATH_MAX] = { 0, };

    ret = UNIFYFS_REAL(stat)(__conpath(conpath, path), buf);
    if (ret < 0) {
        return ret;
    }

    ret = unifyfs_invoke_lsm_stat(buf->st_ino, &sb);
    if (ret) {
        LOGERR("lsm_stat failed (ret=%d)", ret);
    }

    buf->st_size = sb.st_size;
    buf->st_atime = sb.st_atime;
    buf->st_mtime = sb.st_mtime;
    buf->st_ctime = sb.st_ctime;

    return 0;
}

off_t unifyfs_storage_lseek(int fd, off_t offset, int whence)
{
    return UNIFYFS_REAL(lseek)(fd, offset, whence);
}

ssize_t unifyfs_storage_write(int fd, const void *buf, size_t count)
{
    return UNIFYFS_REAL(write)(fd, buf, count);
}

static inline int storage_local_read(int fd, size_t count)
{
    uint32_t i = 0;
    size_t len = 0;
    off_t offset = 0;
    struct unifyfs_fmap *fmap = NULL;
    struct unifyfs_fmap_extent *e = NULL;
    int local_rank = client_rank / local_rank_cnt;

    offset = lseek(fd, 0, SEEK_CUR);

    fmap = unifyfs_fd_get_fmap(fd);

    for (i = 0; i < fmap->count; i++) {
        e = &fmap->extents[i];

        if (e->offset <= offset && offset < e->offset + e->length) {
            if (e->rank != local_rank) {
                return 0;
            }

            len = offset - e->offset;
            break;
        }

        e = NULL;
    }

    if (!e) {
        LOGDBG("application trying to read outside the fmap");
        return 0;
    } else {
        for (len = 0, i++ ; i < fmap->count && len < count; i++) {
            e = &fmap->extents[i];
            if (e->rank != local_rank) {
                return 0;
            }

            len += e->length;
        }

        return 1;
    }
}

ssize_t unifyfs_storage_read(int fd, void *buf, size_t count)
{
    ssize_t ret = 0;

    ret = UNIFYFS_REAL(read)(fd, buf, count);

#if 0
    if (storage_local_read(fd, count)) {
        LOGDBG("local read request, read data directly");
        ret = UNIFYFS_REAL(read)(fd, buf, count);
    } else {
        LOGDBG("remote read not implemented, but will fake success");
        ret = count;
    }
#endif

    return ret;
}

int unifyfs_storage_fsync(int fd)
{
    int ret = 0;

    ret = UNIFYFS_REAL(fsync)(fd);
    if (ret < 0) {
        return errno;
    }
    return ret;
}

int unifyfs_storage_fdatasync(int fd)
{
    int ret = 0;

    return ret;
}

int unifyfs_storage_mkdir(const char *path, mode_t mode)
{
    char conpath[PATH_MAX] = { 0, };

    return UNIFYFS_REAL(mkdir)(__conpath(conpath, path), mode);
}

