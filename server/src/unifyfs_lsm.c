/*
 * Copyright (c) 2019, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * LLNL-CODE-741539
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "unifyfs_global.h"
#include "unifyfs_mds.h"
#include "unifyfs_lsm.h"

const char *lsm_root;

/* keeps track of file opened by clients */
struct unifyfs_file {
    const char *rpath;               /* pathname in the backend */
    uint64_t ino;                    /* inode # from localfs */
    int refs;                        /* reference counter */
    struct unifyfs_fmap *fmap;       /* shm fmap */
};

typedef struct unifyfs_file unifyfs_file_t;

#define UNIFYFS_MAX_FILETAB_SIZE 64

static int filetab_count;
static pthread_rwlock_t _filetab_lock = PTHREAD_RWLOCK_INITIALIZER;
static unifyfs_file_t filetab[UNIFYFS_MAX_FILETAB_SIZE];

static inline int filetab_rdlock(void)
{
    return pthread_rwlock_rdlock(&_filetab_lock);
}

static inline int filetab_wrlock(void)
{
    return pthread_rwlock_wrlock(&_filetab_lock);
}

static inline int filetab_unlock(void)
{
    return pthread_rwlock_unlock(&_filetab_lock);
}

static inline
void __filetab_new_file(unifyfs_file_t *file, uint64_t ino, const char *path)
{
    file->rpath = strdup(path);
    assert(file->rpath);

    file->ino = ino;
    file->refs = 1;

    filetab_count++;
}

static const char *filetab_rpath_from_ino(uint64_t ino)
{
    int i = 0;
    unifyfs_file_t *file = NULL;
    const char *rpath = NULL;

    filetab_rdlock();
    {
        for (i = 0; i < filetab_count; i++) {
            file = &filetab[i];

            if (file->ino == ino) {
                rpath = file->rpath;
                goto out_unlock;
            }
        }
    }
out_unlock:
    filetab_unlock();

    return rpath;
}

const char *unifyfs_conpath_from_ino(uint64_t ino)
{
    const char *rpath = filetab_rpath_from_ino(ino);

    return &rpath[strlen(lsm_root)];
}

const char *unifyfs_rpath_from_conpath(const char *conpath)
{
    return &conpath[strlen(lsm_root)];
}

static int filetab_ref(uint64_t ino, const char *rpath)
{
    int ret = 0;
    int i = 0;
    unifyfs_file_t *file = NULL;

    filetab_wrlock();
    {
        for (i = 0; i < filetab_count; i++) {
            file = &filetab[i];

            if (file->ino == ino) {
                file->refs += 1;
                LOGDBG("[filetab] increment ref (ino=%lu, ref=%d)",
                        ino, file->refs);

                goto out_unlock;
            }
        }

        if (filetab_count == UNIFYFS_MAX_FILETAB_SIZE) {
            LOGERR("no more slot in filetab");
            ret = ENOMEM;
            goto out_unlock;
        }

        file = &filetab[i];
        __filetab_new_file(&filetab[i], ino, rpath);

        LOGDBG("[filetab] create new ref (ino=%lu, ref=%d)", ino, file->refs);
    }
out_unlock:
    filetab_unlock();

    return ret;
}

static int filetab_unref(uint64_t ino)
{
    int ret = 0;
    int i = 0;
    unifyfs_file_t *file = NULL;

    filetab_wrlock();
    {
        for (i = 0; i < filetab_count; i++) {
            file = &filetab[i];

            if (file->ino == ino) {
                if (0 == file->refs) {
                    ret = -1;
                    goto out_unlock;
                }

                file->refs -= 1;

                LOGDBG("[filetab] decrement ref (ino=%lu, ref=%d)",
                        ino, file->refs);

                ret = file->refs;

                goto out_unlock;
            }
        }
    }
out_unlock:
    filetab_unlock();

    return ret;
}

static int __filetab_set_fmap(uint64_t ino, struct unifyfs_fmap *fmap)
{
    int ret = ENOENT;
    int i = 0;
    unifyfs_file_t *file = NULL;

    for (i = 0; i < filetab_count; i++) {
        file = &filetab[i];

        if (file->ino == ino) {
            file->fmap = fmap;
            ret = 0;
        }
    }

    return ret;
}


static inline int filetab_set_fmap(uint64_t ino, struct unifyfs_fmap *fmap)
{
    int ret = 0;

    filetab_wrlock();
    {
        ret = __filetab_set_fmap(ino, fmap);
    }
    filetab_unlock();

    return ret;
}

static struct unifyfs_fmap *__filetab_get_fmap(uint64_t ino)
{
    int i = 0;
    unifyfs_file_t *file = NULL;
    struct unifyfs_fmap *fmap = NULL;

    for (i = 0; i < filetab_count; i++) {
        file = &filetab[i];

        if (file->ino == ino) {
            fmap = file->fmap;
            break;
        }
    }

    return fmap;
}

static inline struct unifyfs_fmap *filetab_get_fmap(uint64_t ino)
{
    struct unifyfs_fmap *fmap = NULL;

    filetab_rdlock();
    {
        fmap = __filetab_get_fmap(ino);
    }
    filetab_unlock();

    return fmap;
}

static int lsm_map_fmap(uint64_t ino, const char *pathname)
{
    int ret = 0;
    int fd = 0;
    void *addr = NULL;
    size_t len = 0;
    char fmap_name[PATH_MAX] = { 0, };
    struct unifyfs_fmap *fmap = NULL;

    fmap = filetab_get_fmap(ino);
    if (fmap) {
        LOGDBG("fmap already exists for %s (ino=%lu)", pathname, ino);
        return 0;
    }

    /* we need to map the filemap */
    LOGDBG("need to map a new fmap for %s (ino=%lu)", pathname, ino);

    ret = unifyfs_mds_getfmap(pathname, &fmap);
    if (ret) {
        LOGERR("failed to get fmap (ret=%d)", ret);
        return ret;
    }

#ifdef UNIFYFS_FMAP_DEBUG
    unifyfs_fmap_print(fmap);
#endif

    filetab_wrlock();
    {
        unifyfs_fmap_shm_name(fmap_name, ino);

        fd = shm_open(fmap_name, O_RDWR|O_CREAT, 0644);
        if (fd < 0) {
            if (EEXIST == errno) {
                ret = 0;
                goto out_unlock;
            }

            LOGERR("failed to create fmap shm (%s)", strerror(errno));
            ret = errno;
            goto out_unlock;
        }

        len = unifyfs_fmap_size(fmap);
        ret = posix_fallocate(fd, 0, len);
        if (ret) {
            LOGERR("failed to allocate shm (%s)", strerror(ret));
            goto out_unlock;
        }

        addr = mmap(NULL, len, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
        if (MAP_FAILED == addr) {
            LOGERR("failed to map the shm (%s)", strerror(errno));
            ret = errno;
            goto out_unlock;
        }

        close(fd);
        memcpy(addr, fmap, len);

        __filetab_set_fmap(ino, fmap);

        ret = 0;
    }
out_unlock:
    filetab_unlock();

    if (ret) {
        if (fd) {
            close(fd);
            shm_unlink(fmap_name);
        }
    }

    return ret;
}

char *unifyfs_realpath(char *pathbuf, const char *pathname)
{
    sprintf(pathbuf, "%s%s", lsm_root, pathname);

    return pathbuf;
}

int unifyfs_lsm_init(unifyfs_cfg_t *conf)
{
    int ret = 0;
    struct stat sb = { 0, };
    const char *root = conf->logio_spill_dir;

    ret = stat(root, &sb);
    if (ret < 0) {
        LOGERR("%s is not a valid path (%s)", root, strerror(errno));
        return errno;
    }

    lsm_root = realpath(root, NULL);
    if (!lsm_root) {
        LOGERR("%s", strerror(errno));
        return errno;
    }

    LOGDBG("lsm initialized at %s", lsm_root);

    /* TODO:
     * maybe we can put some hidden file for our bookkeeping..
     */

    return 0;
}

int unifyfs_lsm_exit(void)
{
    return 0;
}

static pthread_mutex_t mount_lock = PTHREAD_MUTEX_INITIALIZER;

int unifyfs_lsm_mount(const char *mountpoint, const char *client)
{
    int ret = 0;
    struct stat sb = { 0, };
    char conpath[PATH_MAX] = { 0, };

    unifyfs_realpath(conpath, mountpoint);

    pthread_mutex_lock(&mount_lock);
    {
        LOGDBG("unifyfs mount conpath: %s from %s", conpath, client);

        ret = stat(conpath, &sb);
        if (0 == ret && S_ISDIR(sb.st_mode)) {
            ret = 0;
        } else if (ENOENT == errno) {
            ret = mkdir(conpath, 0755);
            if (errno == EEXIST) {
                ret = 0;
            }
        } else {
            ret = errno;
        }
    }
    pthread_mutex_unlock(&mount_lock);

    LOGDBG("new mountpoint: %s from %s", mountpoint, client);

    return ret;
}

int unifyfs_lsm_open(const char *pathname, int flags, mode_t mode)
{
    int ret = 0;
    int fd = 0;
    struct stat sb = { 0, };
    char conpath[PATH_MAX] = { 0, };

    if (flags & O_CREAT) {
        ret = unifyfs_mds_create(pathname, flags, mode);
        if (ret) {
            LOGERR("mds failed: %s (%d: %s)",
                    pathname, errno, strerror(errno));
            ret = errno;
            goto out;
        }
    } else if (flags == O_RDONLY) {
        /* for now, we assume the file has been created/written/laminated.
         * get the filemap from the mds. */
        LOGDBG("%s with O_RDONLY", pathname);

    } else {
        ret = unifyfs_mds_search(pathname);
        if (ret != 1) {
            LOGERR("mds failed: %s (%d: %s)",
                    pathname, errno, strerror(errno));
            ret = errno;
            goto out;
        }

        flags |= O_CREAT;
    }

    fd = open(unifyfs_realpath(conpath, pathname), flags, mode);
    if (fd < 0) {
        LOGERR("cannot open file: %s (%s)", conpath, strerror(errno));
        ret = errno;
        goto out;
    }

    ret = fstat(fd, &sb);
    if (ret < 0) {
        LOGERR("stat failed (%s)", strerror(errno));
        ret = errno;
        goto out;
    }

    close(fd);

    if (flags == O_RDONLY) {
        ret = lsm_map_fmap(sb.st_ino, pathname);
    } else {
        filetab_ref(sb.st_ino, conpath);
    }

out:
    return ret;
}

int unifyfs_lsm_close(uint64_t ino)
{
    int ret = 0;
    int refcount = 0;
    const char *conpath = unifyfs_conpath_from_ino(ino);

    assert(conpath);

    refcount = filetab_unref(ino);
    if (refcount < 0) {
        return 0;   /* O_RDONLY */
    }

    LOGDBG("ref count for file (%lu): %d", ino, refcount);

    if (refcount == 0) { /* no more client holding the file */
        ret = unifyfs_mds_addfmap(conpath);
        if (ret) {
            LOGERR("fail to add fmap (ret=%d)", ret);
        }
    }

    return ret;
}

int unifyfs_lsm_stat(uint64_t ino, struct stat *sb)
{
    int ret = 0;
    const char *conpath = unifyfs_conpath_from_ino(ino);

    assert(conpath);

    if (ret == 0) { /* no more client holding the file */
        ret = unifyfs_mds_stat(conpath, sb);
        if (ret) {
            LOGERR("fail to stat(%s) (ret=%d)", conpath, ret);
        }
    }

    return ret;
}

