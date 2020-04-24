#ifndef __UNIFYFS_MDS
#define __UNIFYFS_MDS

#include <sys/types.h>
#include <sys/stat.h>

#include "unifyfs_fmap.h"

int unifyfs_mds_init(void);

int unifyfs_mds_create(const char *pathname, int flags, int mode);

int unifyfs_mds_search(const char *pathname);

int unifyfs_mds_fsync(const char *pathname, size_t size);

int unifyfs_mds_filelen(const char *pathname, size_t *size);

int unifyfs_mds_addfmap(const char *pathname);

int unifyfs_mds_getfmap(const char *pathname, struct unifyfs_fmap **fmap);

int unifyfs_mds_stat(const char *pathname, struct stat *sb);

#endif /* __UNIFYFS_MDS */
