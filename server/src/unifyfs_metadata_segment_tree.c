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
#include "extent_tree.h"

// TODO: need to define this elswhere -> code duplication
size_t meta_slice_sz;

/* initialize the key-value store */
int unifyfs_segtree_init(unifyfs_cfg_t* cfg)
{
    int rc, ret = UNIFYFS_SUCCESS;
    long range_sz = 0;
    rc = configurator_int_val(cfg->meta_range_size, &range_sz);
    if (rc != 0) {
        return -1;
    }
    meta_slice_sz = (size_t) range_sz;
    return ret;
}

int unifyfs_segtree_finalize(void)
{
    return UNIFYFS_SUCCESS;
}

// New API
/*
 *
 */
int unifyfs_segtree_set_file_attribute(
    int set_size,
    int set_laminate,
    unifyfs_file_attr_t* fattr_ptr)
{
    // no delete in extend tree
    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/* given a global file id, lookup and return file attributes */
int unifyfs_segtree_get_file_attribute(
    int gfid,
    unifyfs_file_attr_t* attr)
{
    // no delete in extend tree
    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

/* given a global file id, delete file attributes */
int unifyfs_segtree_delete_file_attribute(
    int gfid)
{
    // no delete in extend tree
    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

static void treenode_to_keyval(int gfid, 
                               struct extent_tree_node *node,
                               unifyfs_keyval_t* keyval)
{
    unifyfs_key_t* key = &(keyval->key);
    key->gfid   = gfid;
    key->offset = node->start;

    /* fill in value */
    unifyfs_val_t* val = &(keyval->val);
    val->addr           = node->pos;
    val->len            = node->end - node->start + 1;
    val->delegator_rank = node->svr_rank;
    val->app_id         = node->app_id;
    val->rank           = node->cli_id;
}

/*
 *
 */
int unifyfs_segtree_get_file_extents(int num_keys, unifyfs_key_t** keys,
                             int* key_lens, int* num_values,
                             unifyfs_keyval_t** keyvals)
{
    int rc = UNIFYFS_SUCCESS;

    int gfid = keys[0]->gfid;
    LOGDBG("BUCKEYES %s: getting extends for %d\n", __func__, gfid);

    unsigned long no_local = 0;
    unsigned long no_remote = 0;

    struct unifyfs_inode *inode;

    // get the inode for the file
    rc = unifyfs_inode_get(gfid, &inode);
    if (UNIFYFS_SUCCESS != rc) {
        return rc;
    }

    // get the extend tree
    struct extent_tree* local  = inode->local_extents;
    struct extent_tree* remote = inode->remote_extents;

    // count the extends
    if (local)
        no_local  = extent_tree_count(local);

    if (remote)
        no_remote = extent_tree_count(remote);

    unsigned long max_extents = no_local + no_remote;

    // allocate array
    unifyfs_keyval_t *_keyvals = malloc(sizeof(unifyfs_keyval_t) * max_extents);

    int count = 0;

    if (local) {
        /* lock the tree for reading */
        extent_tree_rdlock(local);

        struct extent_tree_node* next = extent_tree_find(local, keys[0]->offset, keys[1]->offset);
        while (next != NULL                   &&
               next->start <= keys[1]->offset &&
               count < max_extents)
        {
            /* got an entry that overlaps with given span */

            /* fill in keyval */
            treenode_to_keyval(gfid, next, &_keyvals[count]);

            /* increment the number of key/values we found */
            count++;

            /* get the next element in the tree */
            next = extent_tree_iter(local, next);
        }

        extent_tree_unlock(local);
    }

    if (remote) {
        /* lock the tree for reading */
        extent_tree_rdlock(remote);

        struct extent_tree_node* next = extent_tree_find(local, keys[0]->offset, keys[1]->offset);
        while (next != NULL                   &&
               next->start <= keys[1]->offset &&
               count < max_extents)
        {
            /* got an entry that overlaps with given span */

            /* fill in keyval */
            treenode_to_keyval(gfid, next, &_keyvals[count]);

            /* increment the number of key/values we found */
            count++;

            /* get the next element in the tree */
            next = extent_tree_iter(remote, next);
        }

        extent_tree_unlock(remote);
    }

    *num_values = count;
    *keyvals = _keyvals;

    return rc;
}

/*
 *
 */
int unifyfs_segtree_set_file_extents(int num_entries,
                             unifyfs_key_t** keys, int* key_lens,
                             unifyfs_val_t** vals, int* val_lens)
{
    /* Can we assume that all extends belong to the same file? */
    int rc = UNIFYFS_SUCCESS;

    for (int i = 0; i < num_entries; i++) {

        int gfid = keys[i]->gfid;

        LOGDBG("BUCKEYES %s: setting extends for %d\n", __func__, gfid);

        /* Create node to define our new range */

        struct extent_tree_node* node = calloc(1, sizeof(struct extent_tree_node));
        if (!node) {
            return ENOMEM;
        }

        node->start    = keys[i]->offset;
        node->end      = keys[i]->offset + vals[i]->len;
        node->svr_rank = vals[i]->rank;
        node->app_id   = vals[i]->app_id;
        node->cli_id   = vals[i]->delegator_rank;
        node->pos      = vals[i]->addr;

        rc = unifyfs_inode_add_local_extents(gfid, 1, node);
        if (UNIFYFS_SUCCESS != rc)
            break;
    }

    return rc;
}

/* delete the listed keys from the file extents */
int unifyfs_segtree_delete_file_extents(
    int num_entries,      /* number of items in keys list */
    unifyfs_key_t** keys, /* list of keys to be deleted */
    int* key_lens)        /* list of byte sizes for keys list items */
{
    // no delete in extend tree
    LOGDBG("%s is called but not implemented yet", __func__);
    return UNIFYFS_ERROR_META;
}

struct unifyfs_metaops meta_ops_segtree = {
    .init                   = unifyfs_segtree_init,
    .finalize               = unifyfs_segtree_finalize,
    .get_file_attribute     = unifyfs_segtree_get_file_attribute,
    .delete_file_attribute  = unifyfs_segtree_delete_file_attribute,
    .set_file_attribute     = unifyfs_segtree_set_file_attribute,
    .get_file_extents       = unifyfs_segtree_get_file_extents,
    .delete_file_extents    = unifyfs_segtree_delete_file_extents,
    .set_file_extents       = unifyfs_segtree_set_file_extents,
};

struct unifyfs_metaops* mops = &meta_ops_segtree;