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

#include <pthread.h>
#include <errno.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

#include "unifyfs_global.h"
#include "margo_server.h"
#include "unifyfs_server_rpcs.h"
#include "unifyfs_group_rpc.h"

#include "unifyfs_fmap.h"
#include "unifyfs_mds.h"
#include "unifyfs_lsm.h"

#define SBMAX(a, b) (a) > (b) ? (a) : (b);

extern int glb_pmi_rank;
extern int glb_pmi_size;

/****************************************************************************
 * core mds: nothing too much, but just manage the shared file information.
 ****************************************************************************/

/*
 * int comparator(const void* p1, const void* p2);
 * Return value meaning
 * <0 The element pointed by p1 goes before the element pointed by p2
 * 0  The element pointed by p1 is equivalent to the element pointed by p2
 * >0 The element pointed by p1 goes after the element pointed by p2
 */
int unifyfs_fmap_extent_compare(const void *_e1, const void *_e2)
{
    struct unifyfs_fmap_extent *e1 = (struct unifyfs_fmap_extent *) _e1;
    struct unifyfs_fmap_extent *e2 = (struct unifyfs_fmap_extent *) _e2;

    if (e1->offset < e2->offset) {
        return -1;
    } else if (e1->offset > e2->offset) {
        return 1;
    } else {
        return 0;
    }
}

//static struct fiemap *mds_sys_fiemap(const char *pathname)
static int mds_sys_fiemap(const char *pathname, struct fiemap **fiemap_out,
                          struct stat *sb_out)
{
    int ret = 0;
    int fd = 0;
    struct fiemap *fiemap = NULL;
    int extents_size = 0;
    struct stat sb = { 0, };

    fd = open(pathname, O_RDONLY);
    if (fd < 0) {
        LOGERR("failed to open file %s (%s)", pathname, strerror(errno));
        return errno;
    }

    ret = fstat(fd, &sb);
    if (ret < 0) {
        LOGERR("stat failed on file %s (%s)", pathname, strerror(errno));
        return errno;
    }

    fiemap = calloc(1, sizeof(*fiemap));
    if (!fiemap) {
        LOGERR("calloc failed (%s)", strerror(errno));
        return errno;
    }

    fiemap->fm_length = ~0;

    /* Find out how many extents there are */
    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        LOGERR("fiemap ioctl() failed (%s)", strerror(errno));
        ret = errno;
        goto out_close;
    }

    /* Read in the extents */
    extents_size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

    /* Resize fiemap to allow us to read in the extents */
    fiemap = realloc(fiemap, sizeof(*fiemap) + extents_size);
    if (!fiemap) {
        ret = errno;
        goto out_close;
    }

    memset(fiemap->fm_extents, 0, extents_size);
    fiemap->fm_extent_count = fiemap->fm_mapped_extents;
    fiemap->fm_mapped_extents = 0;

    if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
        LOGERR("fiemap ioctl() failed (%s)", strerror(errno));
        free(fiemap);
        ret = errno;
    }

    *fiemap_out = fiemap;
    *sb_out = sb;

out_close:
    close(fd);

    return ret; 
}

static
struct unifyfs_fmap *mds_build_fmap(struct fiemap *fiemap, struct stat *sb)
{
    uint32_t i = 0;
    uint32_t j = 0;
    uint64_t len = 0;
    uint64_t count = 0;
    uint64_t current_start = 0;
    uint64_t current_len = 0;
    struct fiemap_extent *extent = NULL;
    struct unifyfs_fmap *fmap = NULL;
    struct unifyfs_fmap_extent *fe = NULL;

    LOGDBG("got sys fiemap with %u extents", fiemap->fm_mapped_extents);

    /*
     * FIXME: can we do with one loop?
     */

    for (i = 0; i < fiemap->fm_mapped_extents; i++) {
        extent = &fiemap->fm_extents[i];

#ifdef UNIFYFS_FMAP_DEBUG
        LOGDBG(" - [%4u]: (0x%16.16llx, 0x%16.16llx)",
                i, extent->fe_logical, extent->fe_length);
#endif

        if (current_start + current_len == extent->fe_logical) {
            current_len += extent->fe_length;
        } else {
            if (current_len > 0) {
                count++;
            }
            current_start = extent->fe_logical;
            current_len = extent->fe_length;

        }
    }

    count++;    /* last entry */

    len = sizeof(*fmap) + count*(sizeof(*fe));
    fmap = (struct unifyfs_fmap *) malloc(len);
    if (fmap) {
        memset(fmap, 0, len);

        fmap->rank = glb_pmi_rank;
        fmap->count = count;
        fmap->sb = *sb;

        current_start = 0;
        current_len = 0;

        for (i = 0; i < fiemap->fm_mapped_extents; i++) {
            extent = &fiemap->fm_extents[i];

            if (current_start + current_len == extent->fe_logical) {
                current_len += extent->fe_length;
            } else {
                if (current_len > 0) {
                    fe = &fmap->extents[j++];
                    fe->rank = glb_pmi_rank;
                    fe->offset = current_start;
                    fe->length = current_len;
                }

                current_start = extent->fe_logical;
                current_len = extent->fe_length;
            }
        }

        fe = &fmap->extents[j++];
        fe->rank = glb_pmi_rank;
        fe->offset = current_start;
        fe->length = current_len;

        assert(fmap->count == j);

        LOGDBG("unifyfs_fmap (%u extents)", fmap->count);
#ifdef UNIFYFS_FMAP_DEBUG
        unifyfs_fmap_print(fmap);
#endif
    }

    return fmap;
}

static int mds_get_fmap(const char *pathname, struct unifyfs_fmap **fmap)
{
    int ret = 0;
    struct fiemap *fiemap = NULL;
    struct unifyfs_fmap *_fmap = NULL;
    struct stat sb = { 0, };

    ret = mds_sys_fiemap(pathname, &fiemap, &sb);
    if (ret) {
        LOGERR("failed to get ioctl(FIEMAP) of %s", pathname);
        return ret;
    }

    LOGDBG("got fiemap for %s, %u extents",
            pathname, fiemap->fm_mapped_extents);

    _fmap = mds_build_fmap(fiemap, &sb);
    if (_fmap) {
        *fmap = _fmap;
    } else {
        LOGERR("faild to build fmap for %s", pathname);
        ret = EIO;
    }

    free(fiemap);

    return ret;
}

struct mds_entry {
    const char *pathname;
    int refs;
    size_t size;
    struct unifyfs_fmap *fmap;
};

typedef struct mds_entry mds_entry_t;

#define MDS_TABLE_MAX_ENTRIES 512

struct mds_table {
    pthread_rwlock_t lock;
    int count;
    mds_entry_t entries[MDS_TABLE_MAX_ENTRIES];
};

typedef struct mds_table mds_table_t;

static mds_table_t _mds_table = {
    .lock = PTHREAD_RWLOCK_INITIALIZER,
};

static mds_table_t *mds_table = &_mds_table;

static inline int mds_table_rdlock(void)
{
    return pthread_rwlock_rdlock(&mds_table->lock);
}

static inline int mds_table_wrlock(void)
{
    return pthread_rwlock_wrlock(&mds_table->lock);
}

static inline int mds_table_unlock(void)
{
    return pthread_rwlock_unlock(&mds_table->lock);
}

static mds_entry_t *__mds_table_search(const char *pathname)
{
    int i = 0;
    int count = mds_table->count;
    mds_entry_t *entry = NULL;

    for (i = 0; i < count; i++) {
        entry = &mds_table->entries[i];

        if (entry->pathname && 0 == strcmp(pathname, entry->pathname)) {
            return entry;
        }
    }

    return NULL;
}

static int mds_table_insert(const char *pathname)
{
    int ret = 0;
    mds_entry_t *entry = NULL;

    mds_table_wrlock();
    {
        if (__mds_table_search(pathname)) {
            ret = EEXIST;
            goto out_unlock;
        }

        if (mds_table->count >= MDS_TABLE_MAX_ENTRIES) {
            ret = ENOMEM;
            goto out_unlock;
        }

        entry = &mds_table->entries[mds_table->count];
        entry->refs = 1;
        entry->pathname = strdup(pathname);
        if (!entry->pathname) {
            ret = ENOMEM;
            goto out_unlock;
        }

        mds_table->count += 1;
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

static int mds_table_search(const char *pathname)
{
    int ret = 0;
    mds_entry_t *e = NULL;

    mds_table_rdlock();
    {
        e = __mds_table_search(pathname);
    }
    mds_table_unlock();

    ret = e ? 1 : 0;

    return ret;
}

static int mds_table_fsync(const char *pathname, size_t size)
{
    int ret = 0;
    mds_entry_t *e = NULL;

    mds_table_wrlock();
    {
        e = __mds_table_search(pathname);
        if (!e) {
            ret = ENOENT;
            goto out_unlock;
        }

        if (e->size < size)
            e->size = size;
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

static int mds_table_filelen(const char *pathname, size_t *size)
{
    int ret = 0;
    mds_entry_t *e = NULL;

    mds_table_rdlock();
    {
        e = __mds_table_search(pathname);
        if (!e) {
            ret = ENOENT;
            goto out_unlock;
        }

        *size =e->size;
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

static
struct unifyfs_fmap *__mds_table_mergefmap(struct unifyfs_fmap *old_fmap,
                                           struct unifyfs_fmap *new_fmap)
{
    int count = 0;
    size_t len = 0;
    struct unifyfs_fmap *fmap = NULL;
    struct unifyfs_fmap_extent *pos = NULL;
    struct stat *sb = NULL;

    count = old_fmap->count + new_fmap->count;
    len = sizeof(struct unifyfs_fmap)
          + count*sizeof(struct unifyfs_fmap_extent);

    fmap = realloc(old_fmap, len);
    if (!fmap) {
        LOGERR("failed to allocate memory for new fmap");
        return NULL;
    }

    pos = &fmap->extents[fmap->count];  /* the last entry */
    memcpy(pos, new_fmap->extents, sizeof(*pos)*new_fmap->count);

    pos = fmap->extents;

    qsort(pos, count, sizeof(*pos), unifyfs_fmap_extent_compare);

    fmap->count = count;
    sb = &fmap->sb;
    /*
     * FIXME: this should be more careful than this
     */
    sb->st_size = SBMAX(old_fmap->sb.st_size, new_fmap->sb.st_size);
    sb->st_blocks = old_fmap->sb.st_blocks + new_fmap->sb.st_blocks;
    sb->st_atime = SBMAX(old_fmap->sb.st_atime, new_fmap->sb.st_atime);
    sb->st_mtime = SBMAX(old_fmap->sb.st_mtime, new_fmap->sb.st_mtime);
    sb->st_ctime = SBMAX(old_fmap->sb.st_ctime, new_fmap->sb.st_ctime);

    return fmap;
}


static
int mds_table_addfmap(const char *pathname, struct unifyfs_fmap *new_fmap)
{
    int ret = 0;
    mds_entry_t *e = NULL;

#ifdef UNIFYFS_FMAP_DEBUG
    unifyfs_fmap_print(new_fmap);
#endif

    mds_table_wrlock();
    {
        e = __mds_table_search(pathname);
        if (!e) {
            ret = ENOENT;
            goto out_unlock;
        }

        if (!e->fmap) {
            e->fmap = new_fmap;
            goto out_unlock;
        }

        e->fmap = __mds_table_mergefmap(e->fmap, new_fmap);
        if (ret) {
            LOGERR("failed to merge fmap (ret=%d)", ret);
            goto out_unlock;
        }

#ifdef UNIFYFS_FMAP_DEBUG
        LOGDBG("fmap for file %s:", pathname);
        unifyfs_fmap_print(e->fmap);
#endif
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

/*
 * TODO: this should only work for laminated file, because the caller can
 * freely access the fmap.
 */
static
int mds_table_getfmap(const char *pathname, struct unifyfs_fmap **fmap_out)
{
    int ret = 0;
    mds_entry_t *e = NULL;

    mds_table_rdlock();
    {
        e = __mds_table_search(pathname);
        if (!e) {
            ret = ENOENT;
            goto out_unlock;
        }

        if (!e->fmap) {
            ret = EINVAL; /* FIXME: what's right errno? */
        } else {
            *fmap_out = e->fmap;
        }
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

#if 0
static int mds_table_remove(const char *pathname)
{
    int ret = 0;

    return ret;
}
#endif


static int mds_table_stat(const char *pathname, struct stat *sb)
{
    int ret = 0;
    mds_entry_t *e = NULL;

    mds_table_rdlock();
    {
        e = __mds_table_search(pathname);
        if (!e || !e->fmap) {
            ret = ENOENT;
            goto out_unlock;
        }

        *sb = e->fmap->sb;
    }
out_unlock:
    mds_table_unlock();

    return ret;
}

static inline int mds_target_rank(const char *pathname)
{
    int sum = 0;
    const char *ch = pathname;

    while (ch[0] != '\0') {
        sum += ch[0];
        ch++;
    }

    return sum % glb_pmi_size;
}

/****************************************************************************
 * rpc: create
 ****************************************************************************/

static void mds_create_handle_rpc(hg_handle_t handle)
{
    hg_return_t hret = 0;
    mds_create_in_t in;
    mds_create_out_t out;

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received request (pathname=%s)", in.pathname);

    out.ret = mds_table_insert(in.pathname);

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_create_handle_rpc);

static int
mds_create_invoke_rpc(int target, const char *pathname, int flags, int mode)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    mds_create_in_t in;
    mds_create_out_t out;

    LOGDBG("sending request to mds %d", target);

    in.pathname = pathname;

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_create_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;

    return ret;
}

int unifyfs_mds_create(const char *pathname, int flags, int mode)
{
    int target = mds_target_rank(pathname);

    LOGDBG("[mds_create] target rank: %d", target);

    if (glb_pmi_rank == target) {
        return mds_table_insert(pathname);
    } else {
        return mds_create_invoke_rpc(target, pathname, flags, mode);
    }
}

/****************************************************************************
 * rpc: search
 ****************************************************************************/

static void mds_search_handle_rpc(hg_handle_t handle)
{
    hg_return_t hret = 0;
    mds_search_in_t in;
    mds_search_out_t out;

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received request (pathname=%s)", in.pathname);

    out.ret = mds_table_search(in.pathname);

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_search_handle_rpc);

static int
mds_search_invoke_rpc(int target, const char *pathname)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    mds_search_in_t in;
    mds_search_out_t out;

    LOGDBG("sending request to mds %d", target);

    in.pathname = pathname;

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_search_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;

    return ret;
}

int unifyfs_mds_search(const char *pathname)
{
    int target = mds_target_rank(pathname);

    LOGDBG("[mds_search] target rank: %d", target);

    if (glb_pmi_rank == target) {
        return mds_table_search(pathname);
    } else {
        return mds_search_invoke_rpc(target, pathname);
    }
}

int unifyfs_mds_init(void)
{
    return 0;
}

/****************************************************************************
 * rpc: fsync
 ****************************************************************************/

static void mds_fsync_handle_rpc(hg_handle_t handle)
{
    hg_return_t hret = 0;
    mds_fsync_in_t in;
    mds_fsync_out_t out;

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received request (pathname=%s, size=%lu)", in.pathname, in.size);

    out.ret = mds_table_fsync(in.pathname, in.size);

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_fsync_handle_rpc);

static int
mds_fsync_invoke_rpc(int target, const char *pathname, size_t size)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    mds_fsync_in_t in;
    mds_fsync_out_t out;

    LOGDBG("sending request to mds %d", target);

    in.pathname = pathname;
    in.size = size;

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_fsync_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;

    return ret;
}

int unifyfs_mds_fsync(const char *pathname, size_t size)
{
    int target = mds_target_rank(pathname);

    LOGDBG("[mds_fsync] target rank: %d", target);

    if (glb_pmi_rank == target) {
        return mds_table_fsync(pathname, size);
    } else {
        return mds_fsync_invoke_rpc(target, pathname, size);
    }
}

/****************************************************************************
 * rpc: filelen
 ****************************************************************************/

static void mds_filelen_handle_rpc(hg_handle_t handle)
{
    hg_return_t hret = 0;
    mds_filelen_in_t in;
    mds_filelen_out_t out;

    int ret = 0;
    size_t size = 0;

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received request (pathname=%s)", in.pathname);

    ret = mds_table_filelen(in.pathname, &size);
    if (ret == 0) {
        out.size = size;
    }
    out.ret = ret;

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_filelen_handle_rpc);

static int
mds_filelen_invoke_rpc(int target, const char *pathname, size_t *size)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    mds_filelen_in_t in;
    mds_filelen_out_t out;

    LOGDBG("sending request to mds %d", target);

    in.pathname = pathname;

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_filelen_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;
    if (ret == 0) {
        *size = out.size;
    }

    return ret;
}

int unifyfs_mds_filelen(const char *pathname, size_t *size)
{
    int target = mds_target_rank(pathname);

    LOGDBG("[mds_filelen] target rank: %d", target);

    if (glb_pmi_rank == target) {
        return mds_table_filelen(pathname, size);
    } else {
        return mds_filelen_invoke_rpc(target, pathname, size);
    }
}

/****************************************************************************
 * rpc: addfmap
 ****************************************************************************/

static inline size_t unifyfs_fmap_bulk_size(struct unifyfs_fmap *fmap)
{
    size_t size = 0;

    if (fmap) {
        size = sizeof(*fmap) + sizeof(fmap->extents[0])*fmap->count;
    }

    return size;
}

static void mds_addfmap_handle_rpc(hg_handle_t handle)
{
    int ret = 0;
    size_t fmap_size = 0;
    struct unifyfs_fmap *fmap = NULL;
    hg_return_t hret = 0;
    mds_addfmap_in_t in;
    mds_addfmap_out_t out;
    hg_bulk_t fmap_bulk;
    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    const struct hg_info* info = margo_get_info(handle);
    hg_addr_t client_address = info->addr;

    fmap_size = in.fmap_size;
    fmap = (struct unifyfs_fmap *) malloc(fmap_size);
    if (!fmap) {
        LOGERR("failed to allocate memory for fmap");
    }

    margo_bulk_create(mid, 1, (void **) &fmap, &fmap_size,
                      HG_BULK_READWRITE, &fmap_bulk);

    margo_bulk_transfer(mid, HG_BULK_PULL, client_address,
                        in.fmap, 0, fmap_bulk, 0, fmap_size);

    LOGDBG("received request (pathname=%s, fmap=%lu bytes)",
            in.pathname, fmap_size);

    ret = mds_table_addfmap(in.pathname, fmap);
    if (ret) {
        LOGERR("failed to add fmap to %s", in.pathname);
    }
    out.ret = ret;

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_addfmap_handle_rpc);

static int
mds_addfmap_invoke_rpc(int target, const char *pathname,
                       struct unifyfs_fmap *fmap)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    hg_size_t bulk_fmap_size;
    hg_bulk_t bulk_fmap;
    mds_addfmap_in_t in;
    mds_addfmap_out_t out;

    LOGDBG("sending request to mds %d", target);

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_addfmap_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    bulk_fmap_size = unifyfs_fmap_bulk_size(fmap);
    margo_bulk_create(unifyfsd_rpc_context->svr_mid, 1,
                      (void **) &fmap, &bulk_fmap_size,
                      HG_BULK_READ_ONLY, &bulk_fmap);

    in.pathname = pathname;
    in.fmap_size = bulk_fmap_size;
    in.fmap = bulk_fmap;

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;

    return ret;
}

int unifyfs_mds_addfmap(const char *pathname)
{
    int ret = 0;
    int target = mds_target_rank(pathname);
    struct unifyfs_fmap *fmap = NULL;
    char rpath[PATH_MAX] = { 0, };

    unifyfs_realpath(rpath, pathname);

    ret = mds_get_fmap(rpath, &fmap);
    if (ret) {
        LOGERR("failed to build fmap for %s (ret=%d)", pathname, ret);
        return ret;
    }

    LOGDBG("[mds_addfmap] target rank: %d", target);

    if (glb_pmi_rank == target) {
        ret = mds_table_addfmap(pathname, fmap);
    } else {
        ret = mds_addfmap_invoke_rpc(target, pathname, fmap);
        free(fmap);
    }

    return ret;
}

/****************************************************************************
 * rpc: getfmap
 ****************************************************************************/

static void mds_getfmap_handle_rpc(hg_handle_t handle)
{
    int ret = 0;
    hg_size_t fmap_size = 0;
    struct unifyfs_fmap *fmap = NULL;
    hg_return_t hret = 0;
    mds_getfmap_in_t in;
    mds_getfmap_out_t out;
    hg_bulk_t fmap_bulk;
    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received getfmap request for %s", in.pathname)

    ret = mds_table_getfmap(in.pathname, &fmap);
    if (ret) {
        LOGERR("failed to get fmap to %s", in.pathname);
    }

    out.ret = ret;
    if (ret) {
        out.fmap_size = 0;
        out.fmap = 0;
    } else {
        fmap_size = unifyfs_fmap_size(fmap);
        margo_bulk_create(mid, 1, (void **) &fmap, &fmap_size,
                          HG_BULK_READ_ONLY, &fmap_bulk);

        out.fmap_size = fmap_size;
        out.fmap = fmap_bulk;
    }

    LOGDBG("sending fmap to the caller");

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_getfmap_handle_rpc);

static int
mds_getfmap_invoke_rpc(int target, const char *pathname,
                       struct unifyfs_fmap **fmap_out)
{
    int ret = 0;
    size_t fmap_size = 0;
    struct unifyfs_fmap *fmap = NULL;
    hg_return_t hret = 0;
    hg_handle_t handle;
    hg_bulk_t fmap_bulk;
    mds_getfmap_in_t in;
    mds_getfmap_out_t out;
    margo_instance_id mid = unifyfsd_rpc_context->svr_mid;

    LOGDBG("sending request to mds %d", target);

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_getfmap_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    in.pathname = pathname;

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;
    if (ret) {
        LOGERR("failed to receive fmap (ret=%d)", ret);
        goto out;
    }

    fmap_size = out.fmap_size;
    fmap = (struct unifyfs_fmap *) malloc(fmap_size);
    if (!fmap) {
        LOGERR("failed to allocate memory for fmap");
    }

    margo_bulk_create(mid, 1, (void **) &fmap, &fmap_size,
                      HG_BULK_READWRITE, &fmap_bulk);

    margo_bulk_transfer(mid, HG_BULK_PULL, glb_servers[target].margo_svr_addr,
                        out.fmap, 0, fmap_bulk, 0, fmap_size);

    *fmap_out = fmap;

out:
    return ret;
}


int unifyfs_mds_getfmap(const char *pathname, struct unifyfs_fmap **fmap_out)
{
    int ret = 0;
    int target = mds_target_rank(pathname);
    struct unifyfs_fmap *fmap = NULL;

    LOGDBG("[mds_getfmap] target rank: %d", target);

    if (glb_pmi_rank == target) {
        ret = mds_table_getfmap(pathname, &fmap);
    } else {
        ret = mds_getfmap_invoke_rpc(target, pathname, &fmap);
    }

    *fmap_out = fmap;

    return ret;
}

/****************************************************************************
 * rpc: stat
 ****************************************************************************/

static void mds_stat_handle_rpc(hg_handle_t handle)
{
    hg_return_t hret = 0;
    mds_stat_in_t in;
    mds_stat_out_t out;

    int ret = 0;
    struct stat sb = { 0, };

    hret = margo_get_input(handle, &in);
    assert(HG_SUCCESS == hret);

    LOGDBG("received request (pathname=%s)", in.pathname);

    ret = mds_table_stat(in.pathname, &sb);
    if (ret == 0) {
        unifyfs_stat_from_sys_stat(&out.statbuf, &sb);
    }
    out.ret = ret;

    hret = margo_respond(handle, &out);
    assert(HG_SUCCESS == hret);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(mds_stat_handle_rpc);

static int
mds_stat_invoke_rpc(int target, const char *pathname, struct stat *sb)
{
    int ret = 0;
    hg_return_t hret = 0;
    hg_handle_t handle;
    mds_stat_in_t in;
    mds_stat_out_t out;

    LOGDBG("sending request to mds %d", target);

    in.pathname = pathname;

    hret = margo_create(unifyfsd_rpc_context->svr_mid,
                        glb_servers[target].margo_svr_addr,
                        unifyfsd_rpc_context->rpcs.mds_stat_id,
                        &handle);
    assert(HG_SUCCESS == hret);

    hret = margo_forward(handle, &in);
    assert(HG_SUCCESS == hret);

    hret = margo_get_output(handle, &out);
    assert(HG_SUCCESS == hret);

    ret = out.ret;
    if (ret == 0) {
        sys_stat_from_unifyfs_stat(sb, &out.statbuf);
    }

    return ret;
}

int unifyfs_mds_stat(const char *pathname, struct stat *sb)
{
    int ret = 0;
    int target = mds_target_rank(pathname);

    LOGDBG("[mds_stat] target rank: %d", target);

    if (glb_pmi_rank == target) {
        ret = mds_table_stat(pathname, sb);
    } else {
        ret = mds_stat_invoke_rpc(target, pathname, sb);
    }

    return ret;
}

