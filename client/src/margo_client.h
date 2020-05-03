#ifndef _MARGO_CLIENT_H
#define _MARGO_CLIENT_H

/********************************************
 * margo_client.h - client-server margo RPCs
 ********************************************/

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <margo.h>

#include "unifyfs_meta.h"
#include "unifyfs_client_rpcs.h"

typedef struct ClientRpcIds {
    hg_id_t attach_id;
    hg_id_t mount_id;
    hg_id_t unmount_id;
    hg_id_t metaset_id;
    hg_id_t metaget_id;
    hg_id_t filesize_id;
    hg_id_t truncate_id;
    hg_id_t unlink_id;
    hg_id_t laminate_id;
    hg_id_t sync_id;
    hg_id_t read_id;
    hg_id_t mread_id;

    /* localfs testing */
    hg_id_t lsm_open_id;
    hg_id_t lsm_close_id;
    hg_id_t lsm_stat_id;
    hg_id_t fsync_id;
    hg_id_t filelen_id;
} client_rpcs_t;

typedef struct ClientRpcContext {
    margo_instance_id mid;
    char* client_addr_str;
    hg_addr_t client_addr;
    hg_addr_t svr_addr;
    client_rpcs_t rpcs;
} client_rpc_context_t;


int unifyfs_client_rpc_init(void);

int unifyfs_client_rpc_finalize(void);

void fill_client_attach_info(unifyfs_attach_in_t* in);
int invoke_client_attach_rpc(void);

void fill_client_mount_info(unifyfs_mount_in_t* in);
int invoke_client_mount_rpc(void);

int invoke_client_unmount_rpc(void);

int invoke_client_metaset_rpc(int create, unifyfs_file_attr_t* f_meta);

int invoke_client_metaget_rpc(int gfid, unifyfs_file_attr_t* f_meta);

int invoke_client_filesize_rpc(int gfid, size_t* filesize);

int invoke_client_truncate_rpc(int gfid, size_t filesize);

int invoke_client_unlink_rpc(int gfid);

int invoke_client_laminate_rpc(int gfid);

int invoke_client_sync_rpc(void);

int invoke_client_read_rpc(int gfid, size_t offset, size_t length);

int invoke_client_mread_rpc(int read_count, size_t size, void* buffer);

/* localfs testing */

int unifyfs_invoke_lsm_open(const char *pathname, int flags, mode_t mode);

int unifyfs_invoke_lsm_close(uint64_t ino);

int unifyfs_invoke_lsm_stat(uint64_t ino, struct stat *sb);

#if 0
int unifyfs_delegate_create(const char *pathname, int flags, mode_t mode);

int unifyfs_delegate_search(const char *pathname);

int unifyfs_delegate_fsync(const char *pathname, size_t size);

int unifyfs_delegate_filelen(const char *pathname, size_t *size);
#endif

#endif // MARGO_CLIENT_H
