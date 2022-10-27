#ifndef __INDEXLIB_STATUS_H
#define __INDEXLIB_STATUS_H

IE_NAMESPACE_BEGIN(misc);

enum Status
{
    OK = 0,
    UNKNOWN = 1,
    FAIL = 2,
    NO_SPACE = 3,
    INVALID_ARGUMENT = 4,
    NOT_FOUND = 101,
    DELETED = 102,
};

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_STATUS_H
