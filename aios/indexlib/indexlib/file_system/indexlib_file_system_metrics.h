#ifndef __INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_H
#define __INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/block_cache_metrics.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemMetrics
{
public:
    IndexlibFileSystemMetrics(
        const StorageMetrics& inMemStorageMetrics, const StorageMetrics& localStorageMetrics);
    IndexlibFileSystemMetrics();
    ~IndexlibFileSystemMetrics();

public:
    const StorageMetrics& GetInMemStorageMetrics() const
    { return mInMemStorageMetrics; }
    const StorageMetrics& GetLocalStorageMetrics() const
    { return mLocalStorageMetrics; }

private:
    StorageMetrics mInMemStorageMetrics;
    StorageMetrics mLocalStorageMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibFileSystemMetrics);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_H
