#ifndef __INDEXLIB_DATA_FETCHER_H
#define __INDEXLIB_DATA_FETCHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class DataFetcher
{
public:
    DataFetcher();
    virtual ~DataFetcher();
public:
    virtual bool IsFinish() = 0;
    virtual size_t RequireLength() = 0;
    virtual void Append(void* buffer, size_t length) = 0;
};

DEFINE_SHARED_PTR(DataFetcher);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DATA_FETCHER_H
