#ifndef __UNIFYFS_SERVER_RPCS_H
#define __UNIFYFS_SERVER_RPCS_H

/*
 * Declarations for server-server margo RPCs
 */

#include <margo.h>
#include <mercury.h>
#include <mercury_proc_string.h>
#include <mercury_types.h>

#include "unifyfs_rpc_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* server_hello_rpc (server => server)
 *
 * say hello from one server to another */
MERCURY_GEN_PROC(server_hello_in_t,
                 ((int32_t)(src_rank))
                 ((hg_const_string_t)(message_str)))
MERCURY_GEN_PROC(server_hello_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(server_hello_rpc)

/* server_request_rpc (server => server)
 *
 * request from one server to another */
MERCURY_GEN_PROC(server_request_in_t,
                 ((int32_t)(src_rank))
                 ((int32_t)(req_id))
                 ((int32_t)(req_tag))
                 ((hg_size_t)(bulk_size))
                 ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(server_request_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(server_request_rpc)

/* chunk_read_request_rpc (server => server)
 *
 * request for chunk reads from another server */
MERCURY_GEN_PROC(chunk_read_request_in_t,
                 ((int32_t)(src_rank))
                 ((int32_t)(app_id))
                 ((int32_t)(client_id))
                 ((int32_t)(req_id))
                 ((int32_t)(num_chks))
                 ((hg_size_t)(bulk_size))
                 ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(chunk_read_request_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(chunk_read_request_rpc)

/* chunk_read_response_rpc (server => server)
 *
 * response to remote chunk reads request */
MERCURY_GEN_PROC(chunk_read_response_in_t,
                 ((int32_t)(src_rank))
                 ((int32_t)(app_id))
                 ((int32_t)(client_id))
                 ((int32_t)(req_id))
                 ((int32_t)(num_chks))
                 ((hg_size_t)(bulk_size))
                 ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(chunk_read_response_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(chunk_read_response_rpc)

/* server collective operations: */

/* extbcast_request_rpc (server => server)
 *
 * initiates extbcast request operation */
MERCURY_GEN_PROC(extbcast_request_in_t,
        ((int32_t)(root))
        ((int32_t)(gfid))
        ((int32_t)(num_extends))
        ((hg_bulk_t)(exttree)))
MERCURY_GEN_PROC(extbcast_request_out_t,
        ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(extbcast_request_rpc)

/*
 * filesize (server => server)
 */
MERCURY_GEN_PROC(filesize_in_t,
                 ((int32_t)(root))
                 ((int32_t)(gfid)))
MERCURY_GEN_PROC(filesize_out_t,
                 ((hg_size_t)(filesize))
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(filesize_rpc)

/*
 * truncate (server => server)
 */
MERCURY_GEN_PROC(truncate_in_t,
                 ((int32_t)(root))
                 ((int32_t)(gfid))
                 ((hg_size_t)(filesize)))
MERCURY_GEN_PROC(truncate_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(truncate_rpc)

/*
 * metaset (server => server)
 */
MERCURY_GEN_PROC(metaset_in_t,
                 ((int32_t)(root))
                 ((int32_t)(gfid))
                 ((int32_t)(create))
                 ((unifyfs_file_attr_t)(attr)))
MERCURY_GEN_PROC(metaset_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(metaset_rpc)

/*
 * unlink (server => server)
 */
MERCURY_GEN_PROC(unlink_in_t,
                 ((int32_t)(root))
                 ((int32_t)(gfid)))
MERCURY_GEN_PROC(unlink_out_t,
                 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(unlink_rpc)

/*
 * localfs testing
 */

MERCURY_GEN_PROC(mds_create_in_t,
        ((hg_const_string_t)(pathname)))
MERCURY_GEN_PROC(mds_create_out_t,
        ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(mds_create_handle_rpc);

MERCURY_GEN_PROC(mds_search_in_t,
        ((hg_const_string_t)(pathname)))
MERCURY_GEN_PROC(mds_search_out_t,
        ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(mds_search_handle_rpc);

MERCURY_GEN_PROC(mds_fsync_in_t,
        ((hg_const_string_t)(pathname))
        ((hg_size_t)(size)))
MERCURY_GEN_PROC(mds_fsync_out_t,
        ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(mds_fsync_handle_rpc);

MERCURY_GEN_PROC(mds_filelen_in_t,
        ((hg_const_string_t)(pathname)))
MERCURY_GEN_PROC(mds_filelen_out_t,
        ((int32_t)(ret))
        ((hg_size_t)(size)))
DECLARE_MARGO_RPC_HANDLER(mds_filelen_handle_rpc);

MERCURY_GEN_PROC(mds_addfmap_in_t,
        ((hg_const_string_t)(pathname))
        ((hg_size_t)(fmap_size))
        ((hg_bulk_t)(fmap)))
MERCURY_GEN_PROC(mds_addfmap_out_t,
        ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(mds_addfmap_handle_rpc);

MERCURY_GEN_PROC(mds_getfmap_in_t,
        ((hg_const_string_t)(pathname)))
MERCURY_GEN_PROC(mds_getfmap_out_t,
        ((int32_t)(ret))
        ((hg_size_t)(fmap_size))
        ((hg_bulk_t)(fmap)))
DECLARE_MARGO_RPC_HANDLER(mds_getfmap_handle_rpc);

MERCURY_GEN_PROC(mds_stat_in_t,
        ((hg_const_string_t)(pathname)))
MERCURY_GEN_PROC(mds_stat_out_t,
        ((int32_t)(ret))
        ((unifyfs_stat_t)(statbuf)))
DECLARE_MARGO_RPC_HANDLER(mds_stat_handle_rpc);


#ifdef __cplusplus
} // extern "C"
#endif

#endif // UNIFYFS_SERVER_RPCS_H
