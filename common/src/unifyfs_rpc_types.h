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

#include <margo.h>
#include <mercury_proc_string.h>
#include <time.h>

#include "unifyfs_meta.h"

/* rpc encode/decode for timespec structs */
typedef struct timespec sys_timespec_t;
MERCURY_GEN_STRUCT_PROC(sys_timespec_t,
    ((uint64_t)(tv_sec))
    ((uint64_t)(tv_nsec))
)

/* rpc encode/decode for unifyfs_file_attr_t */
MERCURY_GEN_STRUCT_PROC(unifyfs_file_attr_t,
    ((int32_t)(gfid))
    ((int32_t)(mode))
    ((int32_t)(uid))
    ((int32_t)(gid))
    ((uint32_t)(is_laminated))
    ((hg_size_t)(size))
    ((sys_timespec_t)(atime))
    ((sys_timespec_t)(ctime))
    ((sys_timespec_t)(mtime))
    ((hg_const_string_t)(filename))
)

#endif /* __UNIFYFS_RPC_TYPES_H */
