#ifndef __INDEXLIB_RANDOM_H
#define __INDEXLIB_RANDOM_H

#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include "indexlib/misc/common.h"

IE_NAMESPACE_BEGIN(misc);

inline uint64_t dev_random()
{
    int fd = 0;
    fd=open("/dev/random",O_RDONLY);

    uint64_t result = 0;
    read(fd, &result, sizeof(result));
    close(fd);
    return result;
}

inline uint64_t dev_urandom()
{
    int fd = 0;
    fd=open("/dev/urandom",O_RDONLY);

    uint64_t result = 0;
    read(fd, &result, sizeof(result));
    close(fd);
    return result;
}

IE_NAMESPACE_END(misc);

#endif
