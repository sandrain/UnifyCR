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

/*
 * Copyright (c) 2017, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Copyright (c) 2017, Florida State University. Contributions from
 * the Computer Architecture and Systems Research Laboratory (CASTL)
 * at the Department of Computer Science.
 *
 * Written by: Teng Wang, Adam Moody, Weikuan Yu, Kento Sato, Kathryn Mohror
 * LLNL-CODE-728877. All rights reserved.
 *
 * This file is part of burstfs.
 * For details, see https://github.com/llnl/burstfs
 * Please read https://github.com/llnl/burstfs/LICENSE for full license text.
 */

/*
 * Copyright (c) 2013, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * code Written by
 *   Raghunath Rajachandrasekar <rajachan@cse.ohio-state.edu>
 *   Kathryn Mohror <kathryn@llnl.gov>
 *   Adam Moody <moody20@llnl.gov>
 * All rights reserved.
 * This file is part of CRUISE.
 * For details, see https://github.com/hpc/cruise
 * Please also read this file LICENSE.CRUISE
 */

#include "unifyfs-internal.h"
#include "unifyfs-fixed.h"
#include "unifyfs_runstate.h"

#include <time.h>
#include <mpi.h>

#ifdef UNIFYFS_GOTCHA
#include "gotcha/gotcha_types.h"
#include "gotcha/gotcha.h"
#include "gotcha_map_unifyfs_list.h"
#endif

#include "unifyfs-storage.h"
#include "unifyfs_client_rpcs.h"
#include "unifyfs_server_rpcs.h"
#include "unifyfs_rpc_util.h"
#include "margo_client.h"
#include "seg_tree.h"

/* avoid duplicate mounts (for now) */
static int unifyfs_mounted = -1;

static int unifyfs_fpos_enabled   = 1;  /* whether we can use fgetpos/fsetpos */

unifyfs_cfg_t client_cfg;

unifyfs_index_buf_t unifyfs_indices;
static size_t unifyfs_index_buf_size;    /* size of metadata log */
unsigned long unifyfs_max_index_entries; /* max metadata log entries */

/* tracks total number of unsync'd segments for all files */
unsigned long unifyfs_segment_count;

int global_rank_cnt;  /* count of world ranks */
int local_rank_cnt;   /* count of app ranks on local host */
int local_rank_idx;   /* index within local ranks */
int client_rank;      /* client-provided rank (for debugging) */

int local_del_cnt = 1; /* count of local servers */

/* shared memory buffer to transfer read replies
 * from server to client */
shm_context* shm_recv_ctx; // = NULL

int unifyfs_app_id;
int unifyfs_client_id;
size_t unifyfs_key_slice_range;

static int unifyfs_use_single_shm = 0;
static int unifyfs_page_size      = 0;

static off_t unifyfs_max_offt;
static off_t unifyfs_min_offt;
static off_t unifyfs_max_long;
static off_t unifyfs_min_long;

/* TODO: moved these to fixed file */
int    unifyfs_max_files;  /* maximum number of files to store */
bool   unifyfs_flatten_writes; /* flatten our writes (true = enabled) */
bool   unifyfs_local_extents;  /* track data extents in client to read local */

#if 0
/* log-based I/O context */
logio_context* logio_ctx;
#endif

/* keep track of what we've initialized */
int unifyfs_initialized = 0;

/* superblock - persistent shared memory region (metadata + data) */
static shm_context* shm_super_ctx;

/* per-file metadata */
static void* free_fid_stack;
unifyfs_filename_t* unifyfs_filelist;
static unifyfs_filemeta_t* unifyfs_filemetas;

/* TODO: metadata spillover is not currently supported */
int unifyfs_spillmetablock = -1;

/* array of file descriptors */
unifyfs_fd_t unifyfs_fds[UNIFYFS_MAX_FILEDESCS];
rlim_t unifyfs_fd_limit;

/* array of file streams */
unifyfs_stream_t unifyfs_streams[UNIFYFS_MAX_FILEDESCS];

/*
 * TODO: the number of open directories clearly won't exceed the number of file
 * descriptors. however, the current UNIFYFS_MAX_FILEDESCS value of 256 will
 * quickly run out. if this value is fixed to be reasonably larger, then we
 * would need a way to dynamically allocate the dirstreams instead of the
 * following fixed size array.
 */

/* array of DIR* streams to be used */
unifyfs_dirstream_t unifyfs_dirstreams[UNIFYFS_MAX_FILEDESCS];

/* stack to track free file descriptor values,
 * each is an index into unifyfs_fds array */
void* unifyfs_fd_stack;

/* stack to track free file streams,
 * each is an index into unifyfs_streams array */
void* unifyfs_stream_stack;

/* stack to track free directory streams,
 * each is an index into unifyfs_dirstreams array */
void* unifyfs_dirstream_stack;

/* mount point information */
char*  unifyfs_mount_prefix;
size_t unifyfs_mount_prefixlen = 0;

/* mutex to lock stack operations */
pthread_mutex_t unifyfs_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

/* single function to route all unsupported wrapper calls through */
int unifyfs_vunsupported(
    const char* fn_name,
    const char* file,
    int line,
    const char* fmt,
    va_list args)
{
    /* print a message about where in the UNIFYFS code we are */
    printf("UNIFYFS UNSUPPORTED: %s() at %s:%d: ", fn_name, file, line);

    /* print string with more info about call, e.g., param values */
    va_list args2;
    va_copy(args2, args);
    vprintf(fmt, args2);
    va_end(args2);

    /* TODO: optionally abort */

    return UNIFYFS_SUCCESS;
}

/* single function to route all unsupported wrapper calls through */
int unifyfs_unsupported(
    const char* fn_name,
    const char* file,
    int line,
    const char* fmt,
    ...)
{
    /* print string with more info about call, e.g., param values */
    va_list args;
    va_start(args, fmt);
    int rc = unifyfs_vunsupported(fn_name, file, line, fmt, args);
    va_end(args);
    return rc;
}

/* returns 1 if two input parameters will overflow their type when
 * added together */
inline int unifyfs_would_overflow_offt(off_t a, off_t b)
{
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (unifyfs_max_offt - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (unifyfs_min_offt - a <= b) {
            return 0;
        }
        return 1;
    }

    /* if a and b are mixed signs or at least one of them is 0,
     * then adding them together will produce a result closer to 0
     * or at least no further away than either value already is */
    return 0;
}

/* returns 1 if two input parameters will overflow their type when
 * added together */
inline int unifyfs_would_overflow_long(long a, long b)
{
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (unifyfs_max_long - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (unifyfs_min_long - a <= b) {
            return 0;
        }
        return 1;
    }

    /* if a and b are mixed signs or at least one of them is 0,
     * then adding them together will produce a result closer to 0
     * or at least no further away than either value already is */
    return 0;
}

/* lock access to shared data structures in superblock */
inline int unifyfs_stack_lock()
{
    if (unifyfs_use_single_shm) {
        return pthread_mutex_lock(&unifyfs_stack_mutex);
    }
    return 0;
}

/* unlock access to shared data structures in superblock */
inline int unifyfs_stack_unlock()
{
    if (unifyfs_use_single_shm) {
        return pthread_mutex_unlock(&unifyfs_stack_mutex);
    }
    return 0;
}

/* sets flag if the path is a special path */
inline int unifyfs_intercept_path(const char* path)
{
    /* don't intecept anything until we're initialized */
    if (!unifyfs_initialized) {
        return 0;
    }

    /* if the path starts with our mount point, intercept it */
    if (strncmp(path, unifyfs_mount_prefix, unifyfs_mount_prefixlen) == 0) {
        return 1;
    }
    return 0;
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
inline int unifyfs_intercept_fd(int* fd)
{
    int oldfd = *fd;

    /* don't intecept anything until we're initialized */
    if (!unifyfs_initialized) {
        return 0;
    }

    if (oldfd < unifyfs_fd_limit) {
        /* this fd is a real system fd, so leave it as is */
        return 0;
    } else if (oldfd < 0) {
        /* this is an invalid fd, so we should not intercept it */
        return 0;
    } else {
        /* this is an fd we generated and returned to the user,
         * so intercept the call and shift the fd */
        int newfd = oldfd - unifyfs_fd_limit;
        *fd = newfd;
        LOGDBG("Changing fd from exposed %d to internal %d", oldfd, newfd);
        return 1;
    }
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
inline int unifyfs_intercept_stream(FILE* stream)
{
    /* don't intecept anything until we're initialized */
    if (!unifyfs_initialized) {
        return 0;
    }

    /* check whether this pointer lies within range of our
     * file stream array */
    unifyfs_stream_t* ptr   = (unifyfs_stream_t*) stream;
    unifyfs_stream_t* start = &(unifyfs_streams[0]);
    unifyfs_stream_t* end   = &(unifyfs_streams[UNIFYFS_MAX_FILEDESCS]);
    if (ptr >= start && ptr < end) {
        return 1;
    }

    return 0;
}

/* given an directory stream, return 1 if we should intercept this
 * fdirecotry, 0 otherwise */
inline int unifyfs_intercept_dirstream(DIR* dirp)
{
    /* don't intecept anything until we're initialized */
    if (!unifyfs_initialized) {
        return 0;
    }

    /* check whether this pointer lies within range of our
     * directory stream array */
    unifyfs_dirstream_t* ptr   = (unifyfs_dirstream_t*) dirp;
    unifyfs_dirstream_t* start = &(unifyfs_dirstreams[0]);
    unifyfs_dirstream_t* end   = &(unifyfs_dirstreams[UNIFYFS_MAX_FILEDESCS]);
    if (ptr >= start && ptr < end) {
        return 1;
    }

    return 0;
}

/* given a path, return the file id */
inline int unifyfs_get_fid_from_path(const char* path)
{
    int i = 0;
    while (i < unifyfs_max_files) {
        if (unifyfs_filelist[i].in_use &&
            strcmp((void*)&unifyfs_filelist[i].filename, path) == 0) {
            LOGDBG("File found: unifyfs_filelist[%d].filename = %s",
                   i, (char*)&unifyfs_filelist[i].filename);
            return i;
        }
        i++;
    }

    /* couldn't find specified path */
    return -1;
}

/* initialize file descriptor structure for given fd value */
int unifyfs_fd_init(int fd)
{
    /* get pointer to file descriptor struct for this fd value */
    unifyfs_fd_t* filedesc = &(unifyfs_fds[fd]);

    /* set fid to -1 to indicate fd is not active,
     * set file position to max value,
     * disable read and write flags */
    filedesc->fid   = -1;
    filedesc->pos   = (off_t) -1;
    filedesc->read  = 0;
    filedesc->write = 0;

    return UNIFYFS_SUCCESS;
}

/* initialize file streams structure for given sid value */
int unifyfs_stream_init(int sid)
{
    /* get pointer to file stream struct for this id value */
    unifyfs_stream_t* s = &(unifyfs_streams[sid]);

    /* record our id so when given a pointer to the stream
     * struct we can easily recover our id value */
    s->sid = sid;

    /* set fd to -1 to indicate stream is not active */
    s->fd = -1;

    return UNIFYFS_SUCCESS;
}

/* initialize directory streams structure for given dirid value */
int unifyfs_dirstream_init(int dirid)
{
    /* get pointer to directory stream struct for this id value */
    unifyfs_dirstream_t* dirp = &(unifyfs_dirstreams[dirid]);

    /* initialize fields in structure */
    memset((void*) dirp, 0, sizeof(*dirp));

    /* record our id so when given a pointer to the stream
     * struct we can easily recover our id value */
    dirp->dirid = dirid;

    /* set fid to -1 to indicate stream is not active */
    dirp->fid = -1;

    return UNIFYFS_SUCCESS;
}

/* given a file descriptor, return the file id */
inline int unifyfs_get_fid_from_fd(int fd)
{
    /* check that file descriptor is within range */
    if (fd < 0 || fd >= UNIFYFS_MAX_FILEDESCS) {
        return -1;
    }

    /* get local file id that file descriptor is assocated with,
     * will be -1 if not active */
    int fid = unifyfs_fds[fd].fid;
    return fid;
}

/* return address of file descriptor structure or NULL if fd is out
 * of range */
inline unifyfs_fd_t* unifyfs_get_filedesc_from_fd(int fd)
{
    if (fd >= 0 && fd < UNIFYFS_MAX_FILEDESCS) {
        unifyfs_fd_t* filedesc = &(unifyfs_fds[fd]);
        return filedesc;
    }
    return NULL;
}

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
unifyfs_filemeta_t* unifyfs_get_meta_from_fid(int fid)
{
    /* check that the file id is within range of our array */
    if (fid >= 0 && fid < unifyfs_max_files) {
        /* get a pointer to the file meta data structure */
        unifyfs_filemeta_t* meta = &unifyfs_filemetas[fid];
        return meta;
    }
    return NULL;
}

int unifyfs_fid_is_laminated(int fid)
{
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    return meta->is_laminated;
}

int unifyfs_fd_is_laminated(int fd)
{
    int fid = unifyfs_get_fid_from_fd(fd);
    return unifyfs_fid_is_laminated(fid);
}

/* ---------------------------------------
 * Operations on file storage
 * --------------------------------------- */

/* allocate and initialize data management resource for file */
static int fid_store_alloc(int fid)
{
    /* get meta data for this file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);

    /* indicate that we're using LOGIO to store data for this file */
    meta->storage = FILE_STORAGE_LOGIO;

    return UNIFYFS_SUCCESS;
}

/* free data management resource for file */
static int fid_store_free(int fid)
{
    /* get meta data for this file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);

    /* set storage type back to NULL */
    meta->storage = FILE_STORAGE_NULL;

    /* Free our write seg_tree */
    if (unifyfs_flatten_writes) {
        seg_tree_destroy(&meta->extents_sync);
    }

    /* Free our extent seg_tree */
    if (unifyfs_local_extents) {
        seg_tree_destroy(&meta->extents);
    }

    return UNIFYFS_SUCCESS;
}

/* =======================================
 * Operations on file ids
 * ======================================= */

/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
int unifyfs_fid_is_dir(int fid)
{
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    if (meta && meta->mode & S_IFDIR) {
        return 1;
    } else {
        /* if it doesn't exist, then it's not a directory? */
        return 0;
    }
}

int unifyfs_gfid_from_fid(const int fid)
{
    /* check that local file id is in range */
    if (fid < 0 || fid >= unifyfs_max_files) {
        return -1;
    }

    /* return global file id, cached in file meta struct */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    return meta->gfid;
}

/* scan list of files and return fid corresponding to target gfid,
 * returns -1 if not found */
int unifyfs_fid_from_gfid(int gfid)
{
    int i;
    for (i = 0; i < unifyfs_max_files; i++) {
        if (unifyfs_filelist[i].in_use &&
            unifyfs_filemetas[i].gfid == gfid) {
            /* found a file id that's in use and it matches
             * the target fid, this is the one */
            return i;
        }
    }
    return -1;
}

/* Given a fid, return the path.  */
const char* unifyfs_path_from_fid(int fid)
{
    unifyfs_filename_t* fname = &unifyfs_filelist[fid];
    if (fname->in_use) {
            return fname->filename;
    }
    return NULL;
}

/* checks to see if a directory is empty
 * assumes that check for is_dir has already been made
 * only checks for full path matches, does not check relative paths,
 * e.g. ../dirname will not work
 * returns 1 for yes it is empty
 * returns 0 for no */
int unifyfs_fid_is_dir_empty(const char* path)
{
    int i = 0;
    while (i < unifyfs_max_files) {
        /* only check this element if it's active */
        if (unifyfs_filelist[i].in_use) {
            /* if the file starts with the path, it is inside of that directory
             * also check to make sure that it's not the directory entry itself */
            char* strptr = strstr(path, unifyfs_filelist[i].filename);
            if (strptr == unifyfs_filelist[i].filename &&
                strcmp(path, unifyfs_filelist[i].filename) != 0) {
                /* found a child item in path */
                LOGDBG("File found: unifyfs_filelist[%d].filename = %s",
                       i, (char*)&unifyfs_filelist[i].filename);
                return 0;
            }
        }

        /* go on to next file */
        i++;
    }

    /* couldn't find any files with this prefix, dir must be empty */
    return 1;
}

/* Return the global (laminated) size of the file */
off_t unifyfs_fid_global_size(int fid)
{
    /* get meta data for this file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    if (NULL != meta) {
        return meta->global_size;
    }
    return (off_t)-1;
}

/*
 * Return the size of the file.  If the file is laminated, return the
 * laminated size.  If the file is not laminated, return the local
 * size.
 */
off_t unifyfs_fid_logical_size(int fid)
{
    /* get meta data for this file */
    if (unifyfs_fid_is_laminated(fid)) {
        return unifyfs_fid_global_size(fid);
    } else {
        /* invoke an rpc to ask the server what the file size is */

        /* get gfid for this file */
        int gfid = unifyfs_gfid_from_fid(fid);

        /* sync any writes to disk before requesting file size */
        unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
        if (meta->needs_sync) {
            /* we have some changes to sync for this file */
            unifyfs_sync(gfid);

            /* just synced writes for this file */
            meta->needs_sync = 0;
        }

        /* get file size for this file */
        size_t filesize;
        int ret = invoke_client_filesize_rpc(gfid, &filesize);
        if (ret != UNIFYFS_SUCCESS) {
            /* failed to get file size */
            return (off_t)-1;
        }
        return (off_t)filesize;
    }
}

/* if we have a local fid structure corresponding to the gfid
 * in question, we attempt the file lookup with the fid method
 * otherwise call back to the rpc */
off_t unifyfs_gfid_filesize(int gfid)
{
    off_t filesize = (off_t)-1;

    /* see if we have a fid for this gfid */
    int fid = unifyfs_fid_from_gfid(gfid);
    if (fid >= 0) {
        /* got a fid, look up file size through that
         * method, since it may avoid a server rpc call */
        filesize = unifyfs_fid_logical_size(fid);
    } else {
        /* no fid for this gfid,
         * look it up with server rpc */
        size_t size;
        int ret = invoke_client_filesize_rpc(gfid, &size);
        if (ret == UNIFYFS_SUCCESS) {
            /* got the file size successfully */
            filesize = size;
        }
    }

    return filesize;
}

/* Return the local (un-laminated) size of the file */
off_t unifyfs_fid_log_size(int fid)
{
    /* get meta data for this file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    if (NULL != meta) {
        return meta->log_size;
    }
    return (off_t)-1;
}

/* Update local metadata for file from global metadata */
int unifyfs_fid_update_file_meta(int fid, unifyfs_file_attr_t* gfattr)
{
    if (NULL == gfattr) {
        return UNIFYFS_FAILURE;
    }

    /* lookup local metadata for file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    if (NULL != meta) {
        /* update lamination state */
        meta->is_laminated = gfattr->is_laminated;
        if (meta->is_laminated) {
            /* update file size */
            meta->global_size = (off_t)gfattr->size;
            LOGDBG("laminated file size is %zu bytes",
                   (size_t)meta->global_size);
        }
        return UNIFYFS_SUCCESS;
    }
    /* else, bad fid */
    return UNIFYFS_FAILURE;
}

/*
 * Set the metadata values for a file (after optionally creating it).
 * The gfid for the file is in f_meta->gfid.
 *
 * gfid:   The global file id on which to set metadata.
 *
 * create: If set to 1, attempt to create the file first.  If the file
 *         already exists, then update its metadata with the values in
 *         gfattr.  If set to 0, and the file does not exist, then
 *         the server will return an error.
 *
 * gfattr: The metadata values to store.
 */
int unifyfs_set_global_file_meta(
    int gfid,   /* file id to set meta data for */
    int create, /* whether to set size/laminated fields (1) or not (0) */
    unifyfs_file_attr_t* gfattr) /* meta data to store for file */
{
    /* check that we have an input buffer */
    if (NULL == gfattr) {
        return UNIFYFS_FAILURE;
    }

    /* force the gfid field value to match the gfid we're
     * submitting this under */
    gfattr->gfid = gfid;

    /* submit file attributes to global key/value store */
    int ret = invoke_client_metaset_rpc(create, gfattr);
    return ret;
}

int unifyfs_get_global_file_meta(int gfid, unifyfs_file_attr_t* gfattr)
{
    /* check that we have an output buffer to write to */
    if (NULL == gfattr) {
        return UNIFYFS_FAILURE;
    }

    /* attempt to lookup file attributes in key/value store */
    unifyfs_file_attr_t fmeta;
    int ret = invoke_client_metaget_rpc(gfid, &fmeta);
    if (ret == UNIFYFS_SUCCESS) {
        /* found it, copy attributes to output struct */
        *gfattr = fmeta;
    }
    return ret;
}

/*
 * Set the metadata values for a file (after optionally creating it),
 * using metadata associated with a given local file id.
 *
 * fid:    The local file id on which to base global metadata values.
 *
 * create: If set to 1, attempt to create the file first.  If the file
 *         already exists, then update its metadata with the values in
 *         gfattr.  If set to 0, and the file does not exist, then
 *         the server will return an error.
 */
int unifyfs_set_global_file_meta_from_fid(int fid, int create)
{
    /* initialize an empty file attributes structure */
    unifyfs_file_attr_t fattr = {0};

    /* lookup local metadata for file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);

    /* copy our file name */
    const char* path = unifyfs_path_from_fid(fid);
    sprintf(fattr.filename, "%s", path);

    /* set global file id */
    fattr.gfid = meta->gfid;

    /* use current time for atime/mtime/ctime */
    struct timespec tp = {0};
    clock_gettime(CLOCK_REALTIME, &tp);
    fattr.atime = tp;
    fattr.mtime = tp;
    fattr.ctime = tp;

    /* copy file mode bits and lamination flag */
    fattr.mode = meta->mode;

    /* these fields are set by server, except when we're creating a
     * new file in which case, we should initialize them both to 0 */
    fattr.is_laminated = 0;
    fattr.size         = 0;

    /* capture current uid and gid */
    fattr.uid = getuid();
    fattr.gid = getgid();

    /* submit file attributes to global key/value store */
    int ret = unifyfs_set_global_file_meta(meta->gfid, create, &fattr);
    return ret;
}

/* allocate a file id slot for a new file
 * return the fid or -1 on error */
int unifyfs_fid_alloc()
{
    unifyfs_stack_lock();
    int fid = unifyfs_stack_pop(free_fid_stack);
    unifyfs_stack_unlock();
    LOGDBG("unifyfs_stack_pop() gave %d", fid);
    if (fid < 0) {
        /* need to create a new file, but we can't */
        LOGERR("unifyfs_stack_pop() failed (%d)", fid);
        return -1;
    }
    return fid;
}

/* return the file id back to the free pool */
int unifyfs_fid_free(int fid)
{
    unifyfs_stack_lock();
    unifyfs_stack_push(free_fid_stack, fid);
    unifyfs_stack_unlock();
    return UNIFYFS_SUCCESS;
}

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
int unifyfs_fid_create_file(const char* path)
{
    int rc;

    /* check that pathname is within bounds */
    size_t pathlen = strlen(path) + 1;
    if (pathlen > UNIFYFS_MAX_FILENAME) {
        return ENAMETOOLONG;
    }

    /* allocate an id for this file */
    int fid = unifyfs_fid_alloc();
    if (fid < 0)  {
        /* was there an error? if so, return it */
        errno = ENOSPC;
        return fid;
    }

    /* mark this slot as in use */
    unifyfs_filelist[fid].in_use = 1;

    /* copy file name into slot */
    strcpy((void*)&unifyfs_filelist[fid].filename, path);
    LOGDBG("Filename %s got unifyfs fd %d",
           unifyfs_filelist[fid].filename, fid);

    /* initialize meta data */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    meta->global_size  = 0;
    meta->log_size     = 0;
    meta->flock_status = UNLOCKED;
    meta->storage      = FILE_STORAGE_NULL;
    meta->gfid         = unifyfs_generate_gfid(path);
    meta->needs_sync   = 0;
    meta->chunks       = 0;
    meta->is_laminated = 0;
    meta->mode         = UNIFYFS_STAT_DEFAULT_FILE_MODE;

    if (unifyfs_flatten_writes) {
        /* Initialize our segment tree that will record our writes */
        rc = seg_tree_init(&meta->extents_sync);
        if (rc != 0) {
            errno = rc;
            fid = -1;
        }
    }

    /* Initialize our segment tree to track extents for all writes
     * by this process, can be used to read back local data */
    if (unifyfs_local_extents) {
        rc = seg_tree_init(&meta->extents);
        if (rc != 0) {
            errno = rc;
            fid = -1;
        }
    }

    /* PTHREAD_PROCESS_SHARED allows Process-Shared Synchronization */
    pthread_spin_init(&meta->fspinlock, PTHREAD_PROCESS_SHARED);

    return fid;
}

int unifyfs_fid_create_directory(const char* path)
{
    /* check that pathname is within bounds */
    size_t pathlen = strlen(path) + 1;
    if (pathlen > UNIFYFS_MAX_FILENAME) {
        return (int) ENAMETOOLONG;
    }

    /* get local and global file ids */
    int fid  = unifyfs_get_fid_from_path(path);
    int gfid = unifyfs_generate_gfid(path);

    /* test whether we have info for file in our local file list */
    int found_local = (fid >= 0);

    /* test whether we have metadata for file in global key/value store */
    int found_global = 0;
    unifyfs_file_attr_t gfattr = { 0, };
    if (unifyfs_get_global_file_meta(gfid, &gfattr) == UNIFYFS_SUCCESS) {
        found_global = 1;
    }

    /* can't create if it already exists */
    if (found_global) {
        return (int) EEXIST;
    }

    if (found_local) {
        /* exists locally, but not globally
         *
         * FIXME: so, we have detected the cache inconsistency here.
         * we cannot simply unlink or remove the entry because then we also
         * need to check whether any subdirectories or files exist.
         *
         * this can happen when
         * - a process created a directory. this process (A) has opened it at
         *   least once.
         * - then, the directory has been deleted by another process (B). it
         *   deletes the global entry without checking any local used entries
         *   in other processes.
         *
         * we currently return EIO, and this needs to be addressed according to
         * a consistency model this fs intance assumes.
         */
        return EIO;
    }

    /* now, we need to create a new directory. */
    fid = unifyfs_fid_create_file(path);
    if (fid < 0) {
        /* FIXME: ENOSPC or EIO? */
        return EIO;
    }

    /* Set as directory */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    meta->mode = (meta->mode & ~S_IFREG) | S_IFDIR;

    /* insert global meta data for directory */
    int ret = unifyfs_set_global_file_meta_from_fid(fid, 1);
    if (ret != UNIFYFS_SUCCESS) {
        LOGERR("Failed to populate the global meta entry for %s (fid:%d)",
               path, fid);
        return EIO;
    }

    return UNIFYFS_SUCCESS;
}

/* Write count bytes from buf into file starting at offset pos.
 *
 * Returns UNIFYFS_SUCCESS, or an error code
 */
int unifyfs_fid_write(int fid, off_t pos, const void* buf, size_t count)
{
    int rc;

    /* short-circuit a 0-byte write */
    if (count == 0) {
        return UNIFYFS_SUCCESS;
    }

    /* get meta for this file id */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);

    /* determine storage type to write file data */
    if (meta->storage == FILE_STORAGE_LOGIO) {
        /* file stored in fixed-size chunks */
        rc = unifyfs_fid_logio_write(fid, meta, pos, buf, count);
    } else {
        /* unknown storage type */
        rc = EIO;
    }

    return rc;
}

/* truncate file id to given length, frees resources if length is
 * less than size and allocates and zero-fills new bytes if length
 * is more than size */
int unifyfs_fid_truncate(int fid, off_t length)
{
    /* get meta data for this file */
    unifyfs_filemeta_t* meta = unifyfs_get_meta_from_fid(fid);
    if (meta->is_laminated) {
        /* Can't truncate a laminated file */
        return EINVAL;
    }

    /* determine file storage type */
    if (meta->storage == FILE_STORAGE_LOGIO) {
        /* invoke truncate rpc */
        int gfid = unifyfs_gfid_from_fid(fid);
        int rc = invoke_client_truncate_rpc(gfid, length);
        if (rc != UNIFYFS_SUCCESS) {
            return rc;
        }

        /* truncate succeeded, update global size to
         * reflect truncated size, note log size is not affected */
        meta->global_size = length;
    } else {
        /* unknown storage type */
        return EIO;
    }

    return UNIFYFS_SUCCESS;
}

/* opens a new file id with specified path, access flags, and permissions,
 * fills outfid with file id and outpos with position for current file pointer,
 * returns UNIFYFS error code
 */
int unifyfs_fid_open(const char* path, int flags, mode_t mode, int* outfid,
                     off_t* outpos)
{
    int ret;

    /* set the pointer to the start of the file */
    off_t pos = 0;

    /* check that pathname is within bounds */
    size_t pathlen = strlen(path) + 1;
    if (pathlen > UNIFYFS_MAX_FILENAME) {
        return ENAMETOOLONG;
    }

    /* check whether this file already exists */
    /*
     * TODO: The test of file existence involves both local and global checks.
     * However, the testing below does not seem to cover all cases. For
     * instance, a globally unlinked file might be still cached locally because
     * the broadcast for cache invalidation has not been implemented, yet.
     */

    /* get local and global file ids */
    int fid  = unifyfs_get_fid_from_path(path);
    int gfid = unifyfs_generate_gfid(path);

    LOGDBG("unifyfs_get_fid_from_path() gave %d (gfid = %d)", fid, gfid);

    /* test whether we have info for file in our local file list */
    int found_local = (fid >= 0);

    /* test whether we have metadata for file in global key/value store */
    int found_global = 0;
    unifyfs_file_attr_t gfattr = { 0, };
    if (unifyfs_get_global_file_meta(gfid, &gfattr) == UNIFYFS_SUCCESS) {
        found_global = 1;
    }

    /*
     * Catch any case where we could potentially want to write to a laminated
     * file.
     */
    if (gfattr.is_laminated &&
        ((flags & (O_CREAT | O_TRUNC | O_APPEND | O_WRONLY)) ||
         ((mode & 0222) && (flags != O_RDONLY)))) {
            LOGDBG("Can't open laminated file %s with a writable flag.", path);
            return EROFS;
    }

    /* possibly, the file still exists in our local cache but globally
     * unlinked. Invalidate the entry
     *
     * FIXME: unifyfs_fid_unlink() always returns success.
     */
    if (found_local && !found_global) {
        LOGDBG("file found locally, but seems to be deleted globally. "
               "invalidating the local cache.");
        unifyfs_fid_unlink(fid);
        return ENOENT;
    }

    /* for all other three cases below, we need to open the file and allocate a
     * file descriptor for the client.
     */
    if (!found_local && found_global) {
        /* file has possibly been created by another process.  We need to
         * create a local meta cache and also initialize the local storage
         * space.
         */

        /* initialize local metadata for this file */
        fid = unifyfs_fid_create_file(path);
        if (fid < 0) {
            /* FIXME: ENFILE or EIO ? */
            LOGERR("failed to create a new file %s", path);
            return EIO;
        }

        /* initialize local storage for this file */
        ret = fid_store_alloc(fid);
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("failed to allocate storage space for file %s (fid=%d)",
                   path, fid);
            return EIO;
        }

        /* initialize global size of file from global metadata */
        unifyfs_fid_update_file_meta(fid, &gfattr);
    } else if (found_local && found_global) {
        /* file exists and is valid.  */
        if ((flags & O_CREAT) && (flags & O_EXCL)) {
            return EEXIST;
        }

        if ((flags & O_DIRECTORY) && !unifyfs_fid_is_dir(fid)) {
            return ENOTDIR;
        }

        if (!(flags & O_DIRECTORY) && unifyfs_fid_is_dir(fid)) {
            return ENOTDIR;
        }

        /* update local metadata from global metadata */
        unifyfs_fid_update_file_meta(fid, &gfattr);

        if ((flags & O_TRUNC) && (flags & (O_RDWR | O_WRONLY))) {
            unifyfs_fid_truncate(fid, 0);
        }

        if (flags & O_APPEND) {
            /* We only support O_APPEND on non-laminated files */
            pos = unifyfs_fid_logical_size(fid);
        }
    } else {
        /* !found_local && !found_global
         * If we reach here, we need to create a brand new file.
         */
        if (!(flags & O_CREAT)) {
            LOGERR("%s does not exist (O_CREAT not given).", path);
            return ENOENT;
        }

        LOGDBG("Creating a new entry for %s.", path);
        LOGDBG("superblock addr = %p; free_fid_stack = %p; filelist = %p",
               shm_super_ctx->addr, free_fid_stack, unifyfs_filelist);

        /* allocate a file id slot for this new file */
        fid = unifyfs_fid_create_file(path);
        if (fid < 0) {
            LOGERR("Failed to create new file %s", path);
            return ENFILE;
        }

        /* initialize the storage for the file */
        int store_rc = fid_store_alloc(fid);
        if (store_rc != UNIFYFS_SUCCESS) {
            LOGERR("Failed to create storage for file %s", path);
            return EIO;
        }

        /* insert file attribute for file in key-value store */
        ret = unifyfs_set_global_file_meta_from_fid(fid, 1);
        if (ret != UNIFYFS_SUCCESS) {
            LOGERR("Failed to populate the global meta entry for %s (fid:%d)",
                   path, fid);
            return EIO;
        }
    }

    /* TODO: allocate a free file descriptor and associate it with fid set
     * in_use flag and file pointer */

    /* return local file id and starting file position */
    *outfid = fid;
    *outpos = pos;

    LOGDBG("UNIFYFS_open generated fd %d for file %s", fid, path);

    return UNIFYFS_SUCCESS;
}

int unifyfs_fid_close(int fid)
{
    /* TODO: clear any held locks */

    /* nothing to do here, just a place holder */
    return UNIFYFS_SUCCESS;
}

/* delete a file id and return file its resources to free pools */
int unifyfs_fid_unlink(int fid)
{
    int rc;

    /* invoke unlink rpc */
    int gfid = unifyfs_gfid_from_fid(fid);
    rc = invoke_client_unlink_rpc(gfid);
    if (rc != UNIFYFS_SUCCESS) {
        /* TODO: if item does not exist globally, but just locally,
         * we still want to delete item locally */
        return rc;
    }

    /* finalize the storage we're using for this file */
    rc = fid_store_free(fid);
    if (rc != UNIFYFS_SUCCESS) {
        /* released strorage for file, but failed to release
         * structures tracking storage, again bail out to keep
         * its file id active */
        return rc;
    }

    /* at this point, we have released all storage for the file,
     * and data structures that track its storage, so we can
     * release the file id itself */

    /* set this file id as not in use */
    unifyfs_filelist[fid].in_use = 0;

    /* add this id back to the free stack */
    rc = unifyfs_fid_free(fid);
    if (rc != UNIFYFS_SUCCESS) {
        /* storage for the file was released, but we hit
         * an error while freeing the file id */
        return rc;
    }

    return UNIFYFS_SUCCESS;
}

/* =======================================
 * Operations to mount/unmount file system
 * ======================================= */

/* -------------
 * static APIs
 * ------------- */

/* The super block is a region of shared memory that is used to
 * persist file system data.  It contains both room for data
 * structures used to track file names, meta data, the list of
 * storage blocks used for each file, and optional blocks.
 * It also contains a fixed-size region for keeping log
 * index entries for each file.
 *
 *  - stack of free local file ids of length max_files,
 *    the local file id is used to index into other data
 *    structures
 *
 *  - array of unifyfs_filename structs, indexed by local
 *    file id, provides a field indicating whether file
 *    slot is in use and if so, the current file name
 *
 *  - array of unifyfs_filemeta structs, indexed by local
 *    file id, records list of storage blocks used to
 *    store data for the file
 *
 *  - array of unifyfs_chunkmeta structs, indexed by local
 *    file id and then by chunk id for recording metadata
 *    of each chunk allocated to a file, including host
 *    storage and id of that chunk within its storage
 *
 *  - stack to track free list of memory chunks
 *
 *  - stack to track free list of spillover chunks
 *
 *  - array of storage chunks of length unifyfs_max_chunks,
 *    if storing data in memory
 *
 *  - count of number of active index entries
 *  - array of index metadata to track physical offset
 *    of logical file data, of length unifyfs_max_index_entries,
 *    entries added during write operations
 */

/* compute memory size of superblock in bytes,
 * critical to keep this consistent with
 * init_superblock_pointers */
static size_t get_superblock_size(void)
{
    size_t sb_size = 0;

    /* header: uint32_t to hold magic number to indicate
     * that superblock is initialized */
    sb_size += sizeof(uint32_t);

    /* free file id stack */
    sb_size += unifyfs_stack_bytes(unifyfs_max_files);

    /* file name struct array */
    sb_size += unifyfs_max_files * sizeof(unifyfs_filename_t);

    /* file metadata struct array */
    sb_size += unifyfs_max_files * sizeof(unifyfs_filemeta_t);

    /* index region size */
    sb_size += unifyfs_page_size;
    sb_size += unifyfs_max_index_entries * sizeof(unifyfs_index_t);

    /* return number of bytes */
    return sb_size;
}

static inline
char* next_page_align(char* ptr)
{
    intptr_t orig = (intptr_t) ptr;
    intptr_t aligned = orig;
    intptr_t offset = orig % unifyfs_page_size;
    if (offset) {
        aligned += (unifyfs_page_size - offset);
    }
    LOGDBG("orig=0x%p, next-page-aligned=0x%p", ptr, (char*)aligned);
    return (char*) aligned;
}

/* initialize our global pointers into the given superblock */
static void init_superblock_pointers(void* superblock)
{
    char* ptr = (char*)superblock;

    /* jump over header (right now just a uint32_t to record
     * magic value of 0xdeadbeef if initialized */
    ptr += sizeof(uint32_t);

    /* stack to manage free file ids */
    free_fid_stack = ptr;
    ptr += unifyfs_stack_bytes(unifyfs_max_files);

    /* record list of file names */
    unifyfs_filelist = (unifyfs_filename_t*)ptr;
    ptr += unifyfs_max_files * sizeof(unifyfs_filename_t);

    /* array of file meta data structures */
    unifyfs_filemetas = (unifyfs_filemeta_t*)ptr;
    ptr += unifyfs_max_files * sizeof(unifyfs_filemeta_t);

    /* record pointer to number of index entries */
    unifyfs_indices.ptr_num_entries = (size_t*)ptr;

    /* pointer to array of index entries */
    ptr += unifyfs_page_size;
    unifyfs_indices.index_entry = (unifyfs_index_t*)ptr;
    ptr += unifyfs_max_index_entries * sizeof(unifyfs_index_t);

    /* compute size of memory we're using and check that
     * it matches what we allocated */
    size_t ptr_size = (size_t)(ptr - (char*)superblock);
    if (ptr_size > shm_super_ctx->size) {
        LOGERR("Data structures in superblock extend beyond its size");
    }
}

/* initialize data structures for first use */
static int init_superblock_structures(void)
{
    int i;
    for (i = 0; i < unifyfs_max_files; i++) {
        /* indicate that file id is not in use by setting flag to 0 */
        unifyfs_filelist[i].in_use = 0;
    }

    /* initialize stack of free file ids */
    unifyfs_stack_init(free_fid_stack, unifyfs_max_files);

    /* initialize count of key/value entries */
    *(unifyfs_indices.ptr_num_entries) = 0;

    LOGDBG("Meta-stacks initialized!");

    return UNIFYFS_SUCCESS;
}

/* create superblock of specified size and name, or attach to existing
 * block if available */
static int init_superblock_shm(size_t super_sz)
{
    char shm_name[SHMEM_NAME_LEN] = {0};

    /* attach shmem region for client's superblock */
    sprintf(shm_name, SHMEM_SUPER_FMTSTR, unifyfs_app_id, unifyfs_client_id);
    shm_context* shm_ctx = unifyfs_shm_alloc(shm_name, super_sz);
    if (NULL == shm_ctx) {
        LOGERR("Failed to attach to shmem superblock region %s", shm_name);
        return UNIFYFS_ERROR_SHMEM;
    }
    shm_super_ctx = shm_ctx;

    /* init our global variables to point to spots in superblock */
    void* addr = shm_ctx->addr;
    init_superblock_pointers(addr);

    /* initialize structures in superblock if it's newly allocated,
     * we depend on shm_open setting all bytes to 0 to know that
     * it is not initialized */
    uint32_t initialized = *(uint32_t*)addr;
    if (initialized == 0) {
        /* not yet initialized, so initialize values within superblock */
        init_superblock_structures();

        /* superblock structure has been initialized,
         * so set flag to indicate that fact */
        *(uint32_t*)addr = (uint32_t)0xDEADBEEF;
    }

    /* return starting memory address of super block */
    return UNIFYFS_SUCCESS;
}

/**
 * Initialize the shared recv memory buffer to receive data from the delegators
 */
static int init_recv_shm(void)
{
    char shm_recv_name[SHMEM_NAME_LEN] = {0};
    size_t shm_recv_size = UNIFYFS_DATA_RECV_SIZE;

    /* get size of shared memory region from configuration */
    char* cfgval = client_cfg.client_recv_data_size;
    if (cfgval != NULL) {
        long l;
        int rc = configurator_int_val(cfgval, &l);
        if (rc == 0) {
            shm_recv_size = (size_t) l;
        }
    }

    /* define file name to shared memory file */
    snprintf(shm_recv_name, sizeof(shm_recv_name),
             SHMEM_DATA_FMTSTR, unifyfs_app_id, unifyfs_client_id);

    /* allocate memory for shared memory receive buffer */
    shm_recv_ctx = unifyfs_shm_alloc(shm_recv_name, shm_recv_size);
    if (NULL == shm_recv_ctx) {
        LOGERR("Failed to create buffer for read replies");
        return UNIFYFS_FAILURE;
    }

    return UNIFYFS_SUCCESS;
}

/**
 * calculate the number of ranks per node
 *
 * sets global variables local_rank_cnt & local_rank_idx
 *
 * @param numTasks: number of tasks in the application
 * @return success/error code
 */
static int CountTasksPerNode(int rank, int numTasks)
{
    char hostname[UNIFYFS_MAX_HOSTNAME];
    char localhost[UNIFYFS_MAX_HOSTNAME];
    int resultsLen = UNIFYFS_MAX_HOSTNAME;
    MPI_Status status;
    int i, j, rc;
    int* local_rank_lst = NULL;

    if (numTasks <= 0) {
        LOGERR("invalid number of tasks");
        return -1;
    }

    rc = MPI_Get_processor_name(localhost, &resultsLen);
    if (rc != 0) {
        LOGERR("failed to get the processor's name");
    }

    if (rank == 0) {
        /* a container of (rank, host) mappings*/
        name_rank_pair_t* host_set =
            (name_rank_pair_t*)calloc(numTasks,
                                      sizeof(name_rank_pair_t));

        strcpy(host_set[0].hostname, localhost);
        host_set[0].rank = 0;

        /*
         * MPI_Recv all hostnames, and compare to local hostname
         */
        for (i = 1; i < numTasks; i++) {
            rc = MPI_Recv(hostname, UNIFYFS_MAX_HOSTNAME,
                          MPI_CHAR, MPI_ANY_SOURCE,
                          MPI_ANY_TAG, MPI_COMM_WORLD,
                          &status);
            if (rc != 0) {
                LOGERR("cannot receive hostnames");
                return -1;
            }
            strcpy(host_set[i].hostname, hostname);
            host_set[i].rank = status.MPI_SOURCE;
        }

        /* sort by hostname */
        qsort(host_set, numTasks, sizeof(name_rank_pair_t),
              compare_name_rank_pair);

        /*
         * rank_cnt: records the number of processes on each node
         * rank_set: the list of ranks for each node
         */
        int** rank_set = (int**)calloc(numTasks, sizeof(int*));
        int* rank_cnt = (int*)calloc(numTasks, sizeof(int));
        int cursor = 0;
        int set_counter = 0;

        for (i = 1; i < numTasks; i++) {
            if (strcmp(host_set[i].hostname,
                       host_set[i - 1].hostname) != 0) {
                // found a different host, so switch to a new set
                rank_set[set_counter] =
                    (int*)calloc((i - cursor), sizeof(int));
                rank_cnt[set_counter] = i - cursor;
                int hiter, riter = 0;
                for (hiter = cursor; hiter < i; hiter++, riter++) {
                    rank_set[set_counter][riter] = host_set[hiter].rank;
                }

                set_counter++;
                cursor = i;
            }
        }

        /* fill rank_cnt and rank_set entry for the last node */
        rank_set[set_counter] = (int*)calloc((i - cursor), sizeof(int));
        rank_cnt[set_counter] = numTasks - cursor;
        j = 0;
        for (i = cursor; i < numTasks; i++, j++) {
            rank_set[set_counter][j] = host_set[i].rank;
        }
        set_counter++;

        /* broadcast the rank_cnt and rank_set information to each rank */
        for (i = 0; i < set_counter; i++) {
            /* send each rank set to all of its ranks */
            for (j = 0; j < rank_cnt[i]; j++) {
                if (rank_set[i][j] != 0) {
                    rc = MPI_Send(&rank_cnt[i], 1, MPI_INT, rank_set[i][j],
                                    0, MPI_COMM_WORLD);
                    if (rc != 0) {
                        LOGERR("cannot send local rank cnt");
                        return -1;
                    }
                    rc = MPI_Send(rank_set[i], rank_cnt[i], MPI_INT,
                                  rank_set[i][j], 0, MPI_COMM_WORLD);
                    if (rc != 0) {
                        LOGERR("cannot send local rank list");
                        return -1;
                    }
                } else {
                    local_rank_cnt = rank_cnt[i];
                    local_rank_lst = (int*)calloc(rank_cnt[i], sizeof(int));
                    memcpy(local_rank_lst, rank_set[i],
                           (local_rank_cnt * sizeof(int)));
                }
            }
        }

        for (i = 0; i < set_counter; i++) {
            free(rank_set[i]);
        }
        free(rank_cnt);
        free(host_set);
        free(rank_set);
    } else {
        /* non-root process - MPI_Send hostname to root node */
        rc = MPI_Send(localhost, UNIFYFS_MAX_HOSTNAME, MPI_CHAR,
                      0, 0, MPI_COMM_WORLD);
        if (rc != 0) {
            LOGERR("cannot send host name");
            return -1;
        }
        /* receive the local rank set count */
        rc = MPI_Recv(&local_rank_cnt, 1, MPI_INT,
                      0, 0, MPI_COMM_WORLD, &status);
        if (rc != 0) {
            LOGERR("cannot receive local rank cnt");
            return -1;
        }
        /* receive the the local rank set */
        local_rank_lst = (int*)calloc(local_rank_cnt, sizeof(int));
        rc = MPI_Recv(local_rank_lst, local_rank_cnt, MPI_INT,
                      0, 0, MPI_COMM_WORLD, &status);
        if (rc != 0) {
            free(local_rank_lst);
            LOGERR("cannot receive local rank list");
            return -1;
        }
    }

    /* sort local ranks by rank */
    qsort(local_rank_lst, local_rank_cnt, sizeof(int), compare_int);
    for (i = 0; i < local_rank_cnt; i++) {
        if (local_rank_lst[i] == rank) {
            local_rank_idx = i;
            break;
        }
    }
    free(local_rank_lst);
    return 0;
}

static int unifyfs_init(void)
{
    int rc;
    int i;
    bool b;
    long l;
    unsigned long long bits;
    char* cfgval;

    if (!unifyfs_initialized) {

#ifdef UNIFYFS_GOTCHA
        /* insert our I/O wrappers using gotcha */
        enum gotcha_error_t result;
        result = gotcha_wrap(wrap_unifyfs_list, GOTCHA_NFUNCS, "unifyfs");
        if (result != GOTCHA_SUCCESS) {
            LOGERR("gotcha_wrap returned %d", (int) result);
        }

        /* check for an errors when registering functions with gotcha */
        for (i = 0; i < GOTCHA_NFUNCS; i++) {
            if (*(void**)(wrap_unifyfs_list[i].function_address_pointer) == 0) {
                LOGERR("This function name failed to be wrapped: %s",
                       wrap_unifyfs_list[i].name);

            }
        }
#endif

        /* as a hack to support fgetpos/fsetpos, we store the value of
         * a void* in an fpos_t so check that there's room and at least
         * print a message if this won't work */
        if (sizeof(fpos_t) < sizeof(void*)) {
            LOGERR("fgetpos/fsetpos will not work correctly");
            unifyfs_fpos_enabled = 0;
        }

        /* look up page size for buffer alignment */
        unifyfs_page_size = getpagesize();

        /* compute min and max off_t values */
        bits = sizeof(off_t) * 8;
        unifyfs_max_offt = (off_t)((1ULL << (bits - 1ULL)) - 1ULL);
        unifyfs_min_offt = (off_t)(-(1ULL << (bits - 1ULL)));

        /* compute min and max long values */
        unifyfs_max_long = LONG_MAX;
        unifyfs_min_long = LONG_MIN;

        /* determine max number of files to store in file system */
        unifyfs_max_files = UNIFYFS_MAX_FILES;
        cfgval = client_cfg.client_max_files;
        if (cfgval != NULL) {
            rc = configurator_int_val(cfgval, &l);
            if (rc == 0) {
                unifyfs_max_files = (int)l;
            }
        }

        /* Determine if we should flatten writes or not */
        unifyfs_flatten_writes = 1;
        cfgval = client_cfg.client_flatten_writes;
        if (cfgval != NULL) {
            rc = configurator_bool_val(cfgval, &b);
            if (rc == 0) {
                unifyfs_flatten_writes = (bool)b;
            }
        }

        /* Determine if we should track all write extents and use them
         * to service read requests if all data is local */
        unifyfs_local_extents = 0;
        cfgval = client_cfg.client_local_extents;
        if (cfgval != NULL) {
            rc = configurator_bool_val(cfgval, &b);
            if (rc == 0) {
                unifyfs_local_extents = (bool)b;
            }
        }

        /* define size of buffer used to cache key/value pairs for
         * data offsets before passing them to the server */
        unifyfs_index_buf_size = UNIFYFS_INDEX_BUF_SIZE;
        cfgval = client_cfg.client_write_index_size;
        if (cfgval != NULL) {
            rc = configurator_int_val(cfgval, &l);
            if (rc == 0) {
                unifyfs_index_buf_size = (size_t)l;
            }
        }
        unifyfs_max_index_entries =
            unifyfs_index_buf_size / sizeof(unifyfs_index_t);

        /* record the max fd for the system */
        /* RLIMIT_NOFILE specifies a value one greater than the maximum
         * file descriptor number that can be opened by this process */
        struct rlimit r_limit;

        if (getrlimit(RLIMIT_NOFILE, &r_limit) < 0) {
            LOGERR("getrlimit failed: errno=%d (%s)", errno, strerror(errno));
            return UNIFYFS_FAILURE;
        }
        unifyfs_fd_limit = r_limit.rlim_cur;
        LOGDBG("FD limit for system = %ld", unifyfs_fd_limit);

        /* initialize file descriptor structures */
        int num_fds = UNIFYFS_MAX_FILEDESCS;
        for (i = 0; i < num_fds; i++) {
            unifyfs_fd_init(i);
        }

        /* initialize file stream structures */
        int num_streams = UNIFYFS_MAX_FILEDESCS;
        for (i = 0; i < num_streams; i++) {
            unifyfs_stream_init(i);
        }

        /* initialize directory stream structures */
        int num_dirstreams = UNIFYFS_MAX_FILEDESCS;
        for (i = 0; i < num_dirstreams; i++) {
            unifyfs_dirstream_init(i);
        }

        /* initialize stack of free fd values */
        size_t free_fd_size = unifyfs_stack_bytes(num_fds);
        unifyfs_fd_stack = malloc(free_fd_size);
        unifyfs_stack_init(unifyfs_fd_stack, num_fds);

        /* initialize stack of free stream values */
        size_t free_stream_size = unifyfs_stack_bytes(num_streams);
        unifyfs_stream_stack = malloc(free_stream_size);
        unifyfs_stack_init(unifyfs_stream_stack, num_streams);

        /* initialize stack of free directory stream values */
        size_t free_dirstream_size = unifyfs_stack_bytes(num_dirstreams);
        unifyfs_dirstream_stack = malloc(free_dirstream_size);
        unifyfs_stack_init(unifyfs_dirstream_stack, num_dirstreams);

        /* determine the size of the superblock */
        size_t shm_super_size = get_superblock_size();

        /* get a superblock of shared memory and initialize our
         * global variables for this block */
        rc = init_superblock_shm(shm_super_size);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("failed to initialize superblock shmem");
            return rc;
        }

        /* create shared memory region for holding data for read replies */
        rc = init_recv_shm();
        if (rc < 0) {
            LOGERR("failed to initialize data recv shmem");
            return UNIFYFS_FAILURE;
        }

#if 0
        /* initialize log-based I/O context */
        rc = unifyfs_logio_init_client(unifyfs_app_id, unifyfs_client_id,
                                       &client_cfg, &logio_ctx);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("failed to initialize log-based I/O (rc = %s)",
                   unifyfs_rc_enum_str(rc));
            return rc;
        }
#endif

        rc = unifyfs_storage_init(client_cfg.logio_spill_dir,
                                  unifyfs_mount_prefix);
        if (rc != UNIFYFS_SUCCESS) {
            LOGERR("Failed to initializa the backend");
            return rc;
        }

        /* remember that we've now initialized the library */
        unifyfs_initialized = 1;
    }

    return UNIFYFS_SUCCESS;
}

/* free resources allocated during unifyfs_init().
 * generally, we do this in reverse order with respect to
 * how things were initialized */
static int unifyfs_finalize(void)
{
    int rc = UNIFYFS_SUCCESS;

    if (!unifyfs_initialized) {
        /* not initialized yet, so we shouldn't call finalize */
        return UNIFYFS_FAILURE;
    }

#if 0
    /* close spillover files */
    unifyfs_logio_close(logio_ctx);
    if (unifyfs_spillmetablock != -1) {
        close(unifyfs_spillmetablock);
        unifyfs_spillmetablock = -1;
    }
#endif

    /* detach from superblock shmem, but don't unlink the file so that
     * a later client can reattach. */
    unifyfs_shm_free(&shm_super_ctx);

    /* unlink and detach from data receive shmem */
    unifyfs_shm_unlink(shm_recv_ctx);
    unifyfs_shm_free(&shm_recv_ctx);

    /* free directory stream stack */
    if (unifyfs_dirstream_stack != NULL) {
        free(unifyfs_dirstream_stack);
        unifyfs_dirstream_stack = NULL;
    }

    /* free file stream stack */
    if (unifyfs_stream_stack != NULL) {
        free(unifyfs_stream_stack);
        unifyfs_stream_stack = NULL;
    }

    /* free file descriptor stack */
    if (unifyfs_fd_stack != NULL) {
        free(unifyfs_fd_stack);
        unifyfs_fd_stack = NULL;
    }

    /* no longer initialized, so update the flag */
    unifyfs_initialized = 0;

    return rc;
}


/* ---------------
 * external APIs
 * --------------- */

/* Fill mount rpc input struct with client-side context info */
void fill_client_mount_info(unifyfs_mount_in_t* in)
{
    in->dbg_rank = client_rank;
    in->mount_prefix = strdup(client_cfg.unifyfs_mountpoint);
}

/* Fill attach rpc input struct with client-side context info */
void fill_client_attach_info(unifyfs_attach_in_t* in)
{
    size_t meta_offset = (char*)unifyfs_indices.ptr_num_entries -
                         (char*)shm_super_ctx->addr;
    size_t meta_size   = unifyfs_max_index_entries
                         * sizeof(unifyfs_index_t);

    in->app_id            = unifyfs_app_id;
    in->client_id         = unifyfs_client_id;
    in->shmem_data_size   = shm_recv_ctx->size;
    in->shmem_super_size  = shm_super_ctx->size;
    in->meta_offset       = meta_offset;
    in->meta_size         = meta_size;
#if 0
    in->logio_mem_size    = logio_ctx->shmem->size;
    in->logio_spill_size  = logio_ctx->spill_sz;
#endif
    in->logio_spill_dir   = strdup(client_cfg.logio_spill_dir);
}

/**
 * mount a file system at a given prefix
 * subtype: 0-> log-based file system;
 * 1->striping based file system, not implemented yet.
 * @param prefix: directory prefix
 * @param size: the number of ranks
 * @param l_app_id: application ID
 * @return success/error code
 */
int unifyfs_mount(const char prefix[], int rank, size_t size,
                  int l_app_id)
{
    int rc;
    int kv_rank, kv_nranks;

    if (-1 != unifyfs_mounted) {
        if (l_app_id != unifyfs_mounted) {
            LOGERR("multiple mount support not yet implemented");
            return UNIFYFS_FAILURE;
        } else {
            LOGDBG("already mounted");
            return UNIFYFS_SUCCESS;
        }
    }

    // record our rank for debugging messages
    client_rank = rank;
    global_rank_cnt = (int)size;

    // print log messages to stderr
    unifyfs_log_open(NULL);

    // initialize configuration
    rc = unifyfs_config_init(&client_cfg, 0, NULL);
    if (rc) {
        LOGERR("failed to initialize configuration.");
        return UNIFYFS_FAILURE;
    }
    client_cfg.ptype = UNIFYFS_CLIENT;

    // set log level from config
    char* cfgval = client_cfg.log_verbosity;
    if (cfgval != NULL) {
        long l;
        rc = configurator_int_val(cfgval, &l);
        if (rc == 0) {
            unifyfs_set_log_level((unifyfs_log_level_t)l);
        }
    }

    // record mountpoint prefix string
    unifyfs_mount_prefix = strdup(prefix);
    unifyfs_mount_prefixlen = strlen(unifyfs_mount_prefix);
    client_cfg.unifyfs_mountpoint = unifyfs_mount_prefix;

    // generate app_id from mountpoint prefix
    unifyfs_app_id = unifyfs_generate_gfid(unifyfs_mount_prefix);
    if (l_app_id != 0) {
        LOGDBG("ignoring passed app_id=%d, using mountpoint app_id=%d",
               l_app_id, unifyfs_app_id);
    }

    // update configuration from runstate file
    rc = unifyfs_read_runstate(&client_cfg, NULL);
    if (rc) {
        LOGERR("failed to update configuration from runstate.");
        return UNIFYFS_FAILURE;
    }

    // initialize k-v store access
    kv_rank = client_rank;
    kv_nranks = size;
    rc = unifyfs_keyval_init(&client_cfg, &kv_rank, &kv_nranks);
    if (rc) {
        LOGERR("failed to update configuration from runstate.");
        return UNIFYFS_FAILURE;
    }
    if ((client_rank != kv_rank) || (size != kv_nranks)) {
        LOGDBG("mismatch on mount vs kvstore rank/size");
    }

    /* compute our local rank on the node,
     * the following call initializes local_rank_{cnt,ndx} */
    rc = CountTasksPerNode(client_rank, size);
    if (rc < 0) {
        LOGERR("cannot get the local rank list.");
        return -1;
    }

    /* open rpc connection to server */
    rc = unifyfs_client_rpc_init();
    if (rc != UNIFYFS_SUCCESS) {
        LOGERR("failed to initialize client RPC");
        return rc;
    }

    /* Call client mount rpc function to get client id */
    LOGDBG("calling mount rpc");
    rc = invoke_client_mount_rpc();
    if (rc != UNIFYFS_SUCCESS) {
        /* If we fail to connect to the server, bail with an error */
        LOGERR("failed to mount to server");
        return rc;
    }

    /* initialize our library using assigned client id, creates shared memory
     * regions (e.g., superblock and data recv) and inits log-based I/O */
    rc = unifyfs_init();
    if (rc != UNIFYFS_SUCCESS) {
        return rc;
    }

#if 0
    /* Call client attach rpc function to register our newly created shared
     * memory and files with server */
    LOGDBG("calling attach rpc");
    rc = invoke_client_attach_rpc();
    if (rc != UNIFYFS_SUCCESS) {
        /* If we fail, bail with an error */
        LOGERR("failed to attach to server");
        unifyfs_finalize();
        return rc;
    }

    /* Once we return from attach, we know the server has attached to our
     * shared memory region for read replies, so we can safely remove the
     * file. The memory region will stay active until both client and server
     * unmap them. We keep the superblock file around so that a future client
     * can reattach to it. */
    unifyfs_shm_unlink(shm_recv_ctx);

    /* add mount point as a new directory in the file list */
    if (unifyfs_get_fid_from_path(prefix) < 0) {
        /* no entry exists for mount point, so create one */
        int fid = unifyfs_fid_create_directory(prefix);
        if (fid < 0) {
            /* if there was an error, return it */
            LOGERR("failed to create directory entry for mount point: `%s'",
                   prefix);
            unifyfs_finalize();
            return UNIFYFS_FAILURE;
        }
    }

    /* record client state as mounted for specific app_id */
    unifyfs_mounted = unifyfs_app_id;
#endif

    return UNIFYFS_SUCCESS;
}

/**
 * unmount the mounted file system
 * TODO: Add support for unmounting more than
 * one filesystem.
 * @return success/error code
 */
int unifyfs_unmount(void)
{
    int rc;
    int ret = UNIFYFS_SUCCESS;

    if (-1 == unifyfs_mounted) {
        return UNIFYFS_SUCCESS;
    }

    /************************
     * tear down connection to server
     ************************/

    /* invoke unmount rpc to tell server we're disconnecting */
    LOGDBG("calling unmount");
    rc = invoke_client_unmount_rpc();
    if (rc) {
        LOGERR("client unmount rpc failed");
        ret = UNIFYFS_FAILURE;
    }

    /* free resources allocated in client_rpc_init */
    unifyfs_client_rpc_finalize();

    /************************
     * free our mount point, and detach from structures
     * storing data
     ************************/

    /* free resources allocated in unifyfs_init */
    unifyfs_finalize();

    /* free memory tracking our mount prefix string */
    if (unifyfs_mount_prefix != NULL) {
        free(unifyfs_mount_prefix);
        unifyfs_mount_prefix = NULL;
        unifyfs_mount_prefixlen = 0;
        client_cfg.unifyfs_mountpoint = NULL;
    }

    /************************
     * free configuration values
     ************************/

    /* clean up configuration */
    rc = unifyfs_config_fini(&client_cfg);
    if (rc) {
        LOGERR("unifyfs_config_fini() failed");
        ret = UNIFYFS_FAILURE;
    }

    /* shut down our logging */
    unifyfs_log_close();

    unifyfs_mounted = -1;

    return ret;
}

#define UNIFYFS_TX_BUFSIZE (64*(1<<10))

enum {
    UNIFYFS_TX_STAGE_OUT = 0,
    UNIFYFS_TX_STAGE_IN = 1,
    UNIFYFS_TX_SERIAL = 0,
    UNIFYFS_TX_PARALLEL = 1,
};

static
ssize_t do_transfer_data(int fd_src, int fd_dst, off_t offset, size_t count)
{
    ssize_t ret = 0;
    off_t pos = 0;
    ssize_t n_written = 0;
    ssize_t n_left = 0;
    ssize_t n_processed = 0;
    size_t len = UNIFYFS_TX_BUFSIZE;
    char buf[UNIFYFS_TX_BUFSIZE] = { 0, };

    pos = lseek(fd_src, offset, SEEK_SET);
    if (pos == (off_t) -1) {
        LOGERR("lseek failed (%d: %s)\n", errno, strerror(errno));
        ret = -1;
        goto out;
    }

    pos = lseek(fd_dst, offset, SEEK_SET);
    if (pos == (off_t) -1) {
        LOGERR("lseek failed (%d: %s)\n", errno, strerror(errno));
        ret = -1;
        goto out;
    }

    while (count > n_processed) {
        if (len > count) {
            len = count;
        }

        n_left = read(fd_src, buf, len);

        if (n_left == 0) {  /* EOF */
            break;
        } else if (n_left < 0) {   /* error */
            ret = errno;
            goto out;
        }

        do {
            n_written = write(fd_dst, buf, n_left);

            if (n_written < 0) {
                ret = errno;
                goto out;
            } else if (n_written == 0 && errno && errno != EAGAIN) {
                ret = errno;
                goto out;
            }

            n_left -= n_written;
            n_processed += n_written;
        } while (n_left);
    }

out:
    return ret;
}

static int do_transfer_file_serial(const char* src, const char* dst,
                                   struct stat* sb_src, int dir)
{
    int ret = 0;
    int fd_src = 0;
    int fd_dst = 0;

    /*
     * for now, we do not use the @dir hint.
     */

    fd_src = open(src, O_RDONLY);
    if (fd_src < 0) {
        return errno;
    }

    fd_dst = open(dst, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd_dst < 0) {
        ret = errno;
        goto out_close_src;
    }

    ret = do_transfer_data(fd_src, fd_dst, 0, sb_src->st_size);
    if (ret < 0) {
        LOGERR("do_transfer_data failed!");
    } else {
        fsync(fd_dst);
    }

    close(fd_dst);
out_close_src:
    close(fd_src);

    return ret;
}

static int do_transfer_file_parallel(const char* src, const char* dst,
                                     struct stat* sb_src, int dir)
{
    int ret = 0;
    int fd_src = 0;
    int fd_dst = 0;
    uint64_t total_chunks = 0;
    uint64_t chunk_start = 0;
    uint64_t remainder = 0;
    uint64_t n_chunks = 0;
    uint64_t offset = 0;
    uint64_t len = 0;
    uint64_t size = sb_src->st_size;

    fd_src = open(src, O_RDONLY);
    if (fd_src < 0) {
        return errno;
    }

    fd_dst = open(dst, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd_dst < 0) {
        ret = errno;
        goto out_close_src;
    }

    /*
     * if the file is smaller than (rankcount*buffersize), just do with the
     * serial mode.
     *
     * FIXME: is this assumtion fair even for the large rank count?
     */
    if ((UNIFYFS_TX_BUFSIZE * global_rank_cnt) > size) {
        if (client_rank == 0) {
            ret = do_transfer_file_serial(src, dst, sb_src, dir);
            if (ret) {
                LOGERR("do_transfer_file_parallel failed");
            }

            return ret;
        }
    }

    total_chunks = size / UNIFYFS_TX_BUFSIZE;
    if (size % UNIFYFS_TX_BUFSIZE) {
        total_chunks++;
    }

    n_chunks = total_chunks / global_rank_cnt;
    remainder = total_chunks % global_rank_cnt;

    chunk_start = n_chunks * client_rank;
    if (client_rank < remainder) {
        chunk_start += client_rank;
        n_chunks += 1;
    } else {
        chunk_start += remainder;
    }

    offset = chunk_start * UNIFYFS_TX_BUFSIZE;

    if (client_rank == (global_rank_cnt - 1)) {
        len = (n_chunks - 1) * UNIFYFS_TX_BUFSIZE;
        len += size % UNIFYFS_TX_BUFSIZE;
    } else {
        len = n_chunks * UNIFYFS_TX_BUFSIZE;
    }

    LOGDBG("parallel transfer (%d/%d): offset=%lu, length=%lu",
           client_rank, global_rank_cnt,
           (unsigned long) offset, (unsigned long) len);

    ret = do_transfer_data(fd_src, fd_dst, offset, len);

    close(fd_dst);
out_close_src:
    close(fd_src);

    return ret;
}

int unifyfs_transfer_file(const char* src, const char* dst, int parallel)
{
    int ret = 0;
    int dir = 0;
    struct stat sb_src = { 0, };
    mode_t source_file_mode_write_removed;
    struct stat sb_dst = { 0, };
    int unify_src = 0;
    int unify_dst = 0;
    char dst_path[PATH_MAX] = { 0, };
    char* pos = dst_path;
    char* src_path = strdup(src);

    int local_return_val;

    if (!src_path) {
        return -ENOMEM;
    }

    if (unifyfs_intercept_path(src)) {
        dir = UNIFYFS_TX_STAGE_OUT;
        unify_src = 1;
    }

    ret = UNIFYFS_WRAP(stat)(src, &sb_src);
    if (ret < 0) {
        return -errno;
    }

    pos += sprintf(pos, "%s", dst);

    if (unifyfs_intercept_path(dst)) {
        dir = UNIFYFS_TX_STAGE_IN;
        unify_dst = 1;
    }

    ret = UNIFYFS_WRAP(stat)(dst, &sb_dst);
    if (ret == 0 && !S_ISREG(sb_dst.st_mode)) {
        if (S_ISDIR(sb_dst.st_mode)) {
            sprintf(pos, "/%s", basename((char*) src_path));
        } else {
            return -EEXIST;
        }
    }

    if (unify_src + unify_dst != 1) {
        return -EINVAL;
    }

    if (parallel) {
        local_return_val =
	    do_transfer_file_parallel(src_path, dst_path, &sb_src, dir);
    } else {
        local_return_val =
	    do_transfer_file_serial(src_path, dst_path, &sb_src, dir);
    }

    // We know here that one (but not both) of the constituent files
    // is in the unify FS.  We just have to decide if the *destination* file is.
    // If it is, then now that we've transferred it, we'll set it to be readable
    // so that it will be laminated and will be readable by other processes.
    if (unify_dst) {
      // pull the source file's mode bits, remove all the write bits but leave
      // the rest intact and store that new mode.  Now that the file has been
      // copied into the unify file system, chmod the file to the new
      // permission.  When unify senses all the write bits are removed it will
      // laminate the file.
        source_file_mode_write_removed =
	    (sb_src.st_mode) & ~(0222);
        chmod(dst_path, source_file_mode_write_removed);
    }
    return local_return_val;
}

