#ifndef __UNIFYFS_FMAP_H
#define __UNIFYFS_FMAP_H

#include <stdio.h>
#include <sys/types.h>

struct unifyfs_fmap_extent {
    uint32_t rank;       /* server id */
    uint64_t offset;    /* logical file offset */
    uint64_t length;
};

struct unifyfs_fmap {
    uint32_t rank;
    uint32_t count;
    struct stat sb;
    struct unifyfs_fmap_extent extents[0];
};

static inline size_t unifyfs_fmap_size(struct unifyfs_fmap *fmap)
{
    if (!fmap) {
        return 0;
    } else {
        return sizeof(*fmap) + sizeof(fmap->extents[0])*(fmap->count);
    }
}

static inline void unifyfs_fmap_shm_name(char *buf, uint64_t ino)
{
    if (buf) {
        sprintf(buf, "unifyfs-fmap-%lu", ino);
    }
}

#define UNIFYFS_FMAP_DEBUG 1

#ifdef UNIFYFS_FMAP_DEBUG
static inline void unifyfs_fmap_print(struct unifyfs_fmap *fmap)
{
    uint32_t i = 0;
    struct unifyfs_fmap_extent *fe = NULL;

    if (fmap) {
        LOGDBG("unifyfs_fmap (rank=%u, count=%u, size=%lu):",
                fmap->rank, fmap->count, fmap->sb.st_size);

        for (i = 0; i < fmap->count; i++) {
            fe = &fmap->extents[i];
            LOGDBG("- [%4u] (0x%16.16lx, 0x%16.16lx, %u)",
                    i, fe->offset, fe->length, fe->rank);
        }
    }
}
#endif

#endif /* __UNIFYFS_FMAP_H */
