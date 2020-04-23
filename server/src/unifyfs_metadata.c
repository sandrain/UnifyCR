/*
 * Copyright (c) 2017, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 *
 * Copyright 2017-2019, UT-Battelle, LLC.
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
 * Written by: Teng Wang, Adam Moody, Wekuan Yu, Kento Sato, Kathryn Mohror
 * LLNL-CODE-728877. All rights reserved.
 *
 * This file is part of burstfs.
 * For details, see https://github.com/llnl/burstfs
 * Please read https://github.com/llnl/burstfs/LICNSE for full license text.
 */

// NOTE: following two lines needed for nftw(), MUST COME FIRST IN FILE
#define _XOPEN_SOURCE 500
#include <ftw.h>

// common headers
#include "unifyfs_client_rpcs.h"
#include "ucr_read_builder.h"

// server headers
#include "unifyfs_global.h"
#include "unifyfs_metadata.h"

extern struct unifyfs_metaops *mops;

void debug_log_key_val(const char* ctx,
                       unifyfs_key_t* key,
                       unifyfs_val_t* val)
{
    if ((key != NULL) && (val != NULL)) {
        LOGDBG("@%s - key(gfid=%d, offset=%lu), "
               "val(del=%d, len=%lu, addr=%lu, app=%d, rank=%d)",
               ctx, key->gfid, key->offset,
               val->delegator_rank, val->len, val->addr,
               val->app_id, val->rank);
    } else if (key != NULL) {
        LOGDBG("@%s - key(gfid=%d, offset=%lu)",
               ctx, key->gfid, key->offset);
    }
}

int unifyfs_key_compare(unifyfs_key_t* a, unifyfs_key_t* b)
{
    assert((NULL != a) && (NULL != b));
    if (a->gfid == b->gfid) {
        if (a->offset == b->offset) {
            return 0;
        } else if (a->offset < b->offset) {
            return -1;
        } else {
            return 1;
        }
    } else if (a->gfid < b->gfid) {
        return -1;
    } else {
        return 1;
    }
}

void print_fsync_indices(unifyfs_key_t** keys,
                         unifyfs_val_t** vals,
                         size_t num_entries)
{
    size_t i;
    for (i = 0; i < num_entries; i++) {
        LOGDBG("gfid:%d, offset:%lu, addr:%lu, len:%lu, del_id:%d",
               keys[i]->gfid, keys[i]->offset,
               vals[i]->addr, vals[i]->len,
               vals[i]->delegator_rank);
    }
}

int unifyfs_meta_finalize(void)
{
    if (NULL != mops->finalize) {
        return mops->finalize();
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}
int unifyfs_meta_init(unifyfs_cfg_t* cfg)
{
    if (NULL != mops->init) {
        return mops->init(cfg);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

// New API
/*
 *
 */
int unifyfs_set_file_attribute(
    int set_size,
    int set_laminate,
    unifyfs_file_attr_t* fattr_ptr)
{
    if (NULL != mops->set_file_attribute) {
        return mops->set_file_attribute(set_size, set_laminate, fattr_ptr);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

#if 0 // not used?
/*
 *
 */
int unifyfs_set_file_attributes(int num_entries,
                                fattr_key_t** keys, int* key_lens,
                                unifyfs_file_attr_t** fattr_ptr, int* val_lens)
{
    if (NULL != mops->set_file_attributes) {
        return mops->set_file_attributes(num_entries, keys, key_lens, fattr_ptr, val_lens);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}
#endif

/* given a global file id, lookup and return file attributes */
int unifyfs_get_file_attribute(int gfid, unifyfs_file_attr_t* attr)
{
    if (NULL != mops->get_file_attribute) {
        return mops->get_file_attribute(gfid, attr);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/* given a global file id, delete file attributes */
int unifyfs_delete_file_attribute(int gfid)
{
    if (NULL != mops->delete_file_attribute) {
        return mops->delete_file_attribute(gfid);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/*
 *
 */
int unifyfs_get_file_extents(int num_keys, unifyfs_key_t** keys,
                             int* key_lens, int* num_values,
                             unifyfs_keyval_t** keyvals)
{
    if (NULL != mops->get_file_extents) {
        return mops->get_file_extents(num_keys, keys, key_lens, num_values, keyvals);
    }
    
    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/*
 *
 */
int unifyfs_set_file_extents(int num_entries,
                             unifyfs_key_t** keys, int* key_lens,
                             unifyfs_val_t** vals, int* val_lens)
{
    if (NULL != mops->set_file_extents) {
        return mops->set_file_extents(num_entries, keys, key_lens, vals, val_lens);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/* delete the listed keys from the file extents */
int unifyfs_delete_file_extents(
    int num_entries,      /* number of items in keys list */
    unifyfs_key_t** keys, /* list of keys to be deleted */
    int* key_lens)        /* list of byte sizes for keys list items */
{
    if (NULL != mops->delete_file_extents) {
        return mops->delete_file_extents(num_entries, keys, key_lens);
    }

    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}
