/*
 * Copyright (c) 2018, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 *
 * Copyright 2018, UT-Battelle, LLC.
 *
 * LLNL-CODE-741539
 * All rights reserved.
 *
 * This is the license for UnifyFS.
 * For details, see https://github.com/LLNL/UnifyFS.
 * Please read https://github.com/LLNL/UnifyFS/LICENSE for full license text.
 */

#ifndef __UNIFYFS_RPC_TYPES_H
#define __UNIFYFS_RPC_TYPES_H

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <margo.h>
#include <mercury.h>
#include <mercury_proc_string.h>
#include <mercury_types.h>
#include "unifyfs_meta.h"

/* need to transfer timespec structs */
typedef struct timespec sys_timespec_t;
MERCURY_GEN_STRUCT_PROC(sys_timespec_t,
                        ((uint64_t)(tv_sec))
                        ((uint64_t)(tv_nsec)))

typedef struct {
    uint64_t dev;
    uint64_t ino;
    uint64_t mode;
    uint64_t nlink;
    uint64_t uid;
    uint64_t gid;
    uint64_t rdev;
    uint64_t size;
    uint64_t blksize;
    uint64_t blocks;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
} unifyfs_stat_t;

/* sys stat type */
MERCURY_GEN_STRUCT_PROC(unifyfs_stat_t,
                        ((uint64_t)(dev))
                        ((uint64_t)(ino))
                        ((uint64_t)(mode))
                        ((uint64_t)(nlink))
                        ((uint64_t)(uid))
                        ((uint64_t)(gid))
                        ((uint64_t)(rdev))
                        ((uint64_t)(size))
                        ((uint64_t)(blksize))
                        ((uint64_t)(blocks))
                        ((uint64_t)(atime))
                        ((uint64_t)(mtime))
                        ((uint64_t)(ctime)))

static inline
void unifyfs_stat_from_sys_stat(unifyfs_stat_t *usb, struct stat *sb)
{
    if (!usb || !sb)
        return;

    usb->dev = sb->st_dev;
    usb->ino = sb->st_ino;
    usb->mode = sb->st_mode;
    usb->nlink = sb->st_nlink;
    usb->uid = sb->st_uid;
    usb->gid = sb->st_gid;
    usb->rdev = sb->st_rdev;
    usb->size = sb->st_size;
    usb->blksize = sb->st_blksize;
    usb->blocks = sb->st_blocks;
    usb->atime = sb->st_atime;
    usb->mtime = sb->st_mtime;
    usb->ctime = sb->st_ctime;
}

static inline
void sys_stat_from_unifyfs_stat(struct stat *sb, unifyfs_stat_t *usb)
{
    if (!usb || !sb)
        return;

    sb->st_dev = usb->dev;
    sb->st_ino = usb->ino;
    sb->st_mode = usb->mode;
    sb->st_nlink = usb->nlink;
    sb->st_uid = usb->uid;
    sb->st_gid = usb->gid;
    sb->st_rdev = usb->rdev;
    sb->st_size = usb->size;
    sb->st_blksize = usb->blksize;
    sb->st_blocks = usb->blocks;
    sb->st_atime = usb->atime;
    sb->st_mtime = usb->mtime;
    sb->st_ctime = usb->ctime;
}

/* encode/decode unifyfs_file_attr_t */
static inline
hg_return_t hg_proc_unifyfs_file_attr_t(hg_proc_t proc, void* _attr)
{
    int i = 0;
    unifyfs_file_attr_t* attr = (unifyfs_file_attr_t*) _attr;
    hg_return_t ret = HG_SUCCESS;

    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
        ret = hg_proc_int32_t(proc, &attr->gfid);
        for (i = 0; i < UNIFYFS_MAX_FILENAME; i++) {
            ret |= hg_proc_int8_t(proc, &(attr->filename[i]));
        }
        ret |= hg_proc_int32_t(proc, &attr->mode);
        ret |= hg_proc_int32_t(proc, &attr->uid);
        ret |= hg_proc_int32_t(proc, &attr->gid);
        ret |= hg_proc_uint64_t(proc, &attr->size);
        ret |= hg_proc_sys_timespec_t(proc, &attr->atime);
        ret |= hg_proc_sys_timespec_t(proc, &attr->mtime);
        ret |= hg_proc_sys_timespec_t(proc, &attr->ctime);
        ret |= hg_proc_uint32_t(proc, &attr->is_laminated);
        if (ret != HG_SUCCESS) {
            ret = HG_PROTOCOL_ERROR;
        }
        break;

    case HG_DECODE:
        ret = hg_proc_int32_t(proc, &attr->gfid);
        for (i = 0; i < UNIFYFS_MAX_FILENAME; i++) {
            ret |= hg_proc_int8_t(proc, &(attr->filename[i]));
        }
        ret |= hg_proc_int32_t(proc, &attr->mode);
        ret |= hg_proc_int32_t(proc, &attr->uid);
        ret |= hg_proc_int32_t(proc, &attr->gid);
        ret |= hg_proc_uint64_t(proc, &attr->size);
        ret |= hg_proc_sys_timespec_t(proc, &attr->atime);
        ret |= hg_proc_sys_timespec_t(proc, &attr->mtime);
        ret |= hg_proc_sys_timespec_t(proc, &attr->ctime);
        ret |= hg_proc_uint32_t(proc, &attr->is_laminated);
        if (ret != HG_SUCCESS) {
            ret = HG_PROTOCOL_ERROR;
        }
        break;

    case HG_FREE:
    default:
        /* nothing */
        break;
    }

    return ret;
}

#endif /* __UNIFYFS_RPC_TYPES_H */
