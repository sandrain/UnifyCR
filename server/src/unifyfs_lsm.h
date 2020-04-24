#ifndef __UNIFYFS_LSM
#define __UNIFYFS_LSM

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "unifyfs_global.h"


int unifyfs_lsm_init(unifyfs_cfg_t *conf);

int unifyfs_lsm_exit(void);

int unifyfs_lsm_mount(const char *mountpoint, const char *client);

int unifyfs_lsm_open(const char *pathname, int flags, mode_t mode);

int unifyfs_lsm_close(uint64_t ino);

int unifyfs_lsm_stat(uint64_t ino, struct stat *sb);

/*
 * TODO: these functions need to be somewhere else
 */
const char *unifyfs_conpath_from_ino(uint64_t ino);

const char *unifyfs_rpath_from_conpath(const char *conpath);

char *unifyfs_realpath(char *pathbuf, const char *pathname);

#endif /* __UNIFYFS_LSM */
