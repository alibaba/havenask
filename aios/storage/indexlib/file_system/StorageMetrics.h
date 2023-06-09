/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/AtomicCounter.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"

namespace indexlib { namespace file_system {

struct StorageMetricsItem {
    autil::AtomicCounter64 fileLength;
    autil::AtomicCounter64 fileCount;
};

struct FileCacheMetrics {
    autil::AtomicCounter64 hitCount;
    autil::AtomicCounter64 missCount;
    autil::AtomicCounter64 replaceCount;
    autil::AtomicCounter64 removeCount;
};

class StorageMetrics
{
public:
    StorageMetrics() {};
    ~StorageMetrics() {};

public:
    void IncreaseFile(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength);
    void DecreaseFile(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength);

    void IncreaseFileCount(FSMetricGroup metricGroup, FSFileType type);
    void DecreaseFileCount(FSMetricGroup metricGroup, FSFileType type);
    void IncreaseFileLength(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength);
    void DecreaseFileLength(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength);

    void IncreaseFileCacheHit();
    void IncreaseFileCacheMiss();
    void IncreaseFileCacheReplace();
    void IncreaseFileCacheRemove();

    void IncreasePackageFileCount();
    void DecreasePackageFileCount();
    void IncreasePackageInnerFileCount();
    void DecreasePackageInnerFileCount();
    void IncreasePackageInnerDirCount();
    void DecreasePackageInnerDirCount();

    void IncreaseFlushMemoryUse(size_t fileLength);
    void DecreaseFlushMemoryUse(size_t fileLength);

    int64_t GetFileLength(FSMetricGroup metricGroup, FSFileType type) const;
    int64_t GetFileCount(FSMetricGroup metricGroup, FSFileType type) const;
    int64_t GetInMemFileLength(FSMetricGroup metricGroup) const;
    int64_t GetMmapLockFileLength(FSMetricGroup metricGroup) const;
    int64_t GetMmapNonlockFileLength(FSMetricGroup metricGroup) const;
    int64_t GetResourceFileLength(FSMetricGroup metricGroup) const;

    int64_t GetTotalFileLength() const { return _totalFileLength.getValue(); }
    int64_t GetTotalFileCount() const { return _totalFileCount.getValue(); }
    int64_t GetPackageFileCount() const { return _packageFileCount.getValue(); }
    int64_t GetPackageInnerFileCount() const { return _packageInnerFileCount.getValue(); }
    int64_t GetPackageInnerDirCount() const { return _packageInnerDirCount.getValue(); }
    int64_t GetFlushMemoryUse() const { return _flushMemoryUse.getValue(); }

    const FileCacheMetrics& GetFileCacheMetrics() const { return _fileCacheMetrics; }

private:
    StorageMetricsItem _metricsItems[FSMG_UNKNOWN][FSFT_UNKNOWN]; // the index is FSFileType
    FileCacheMetrics _fileCacheMetrics;
    autil::AtomicCounter64 _totalFileLength;
    autil::AtomicCounter64 _totalFileCount;
    autil::AtomicCounter64 _packageFileCount;
    autil::AtomicCounter64 _packageInnerFileCount;
    autil::AtomicCounter64 _packageInnerDirCount;
    autil::AtomicCounter64 _flushMemoryUse;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////
inline void StorageMetrics::IncreaseFile(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    IncreaseFileCount(metricGroup, type);
    IncreaseFileLength(metricGroup, type, fileLength);
}

inline void StorageMetrics::DecreaseFile(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    DecreaseFileCount(metricGroup, type);
    DecreaseFileLength(metricGroup, type, fileLength);
}

inline void StorageMetrics::IncreaseFileCount(FSMetricGroup metricGroup, FSFileType type)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    _metricsItems[metricGroup][type].fileCount.inc();
    _totalFileCount.inc();
}

inline void StorageMetrics::DecreaseFileCount(FSMetricGroup metricGroup, FSFileType type)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    _metricsItems[metricGroup][type].fileCount.dec();
    _totalFileCount.dec();
}

inline void StorageMetrics::IncreaseFileLength(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength)
{
    _metricsItems[metricGroup][type].fileLength.add(fileLength);
    _totalFileLength.add(fileLength);
}

inline void StorageMetrics::DecreaseFileLength(FSMetricGroup metricGroup, FSFileType type, int64_t fileLength)
{
    _metricsItems[metricGroup][type].fileLength.sub(fileLength);
    _totalFileLength.sub(fileLength);
}

inline void StorageMetrics::IncreaseFileCacheHit() { _fileCacheMetrics.hitCount.inc(); }

inline void StorageMetrics::IncreaseFileCacheMiss() { _fileCacheMetrics.missCount.inc(); }

inline void StorageMetrics::IncreaseFileCacheReplace() { _fileCacheMetrics.replaceCount.inc(); }

inline void StorageMetrics::IncreaseFileCacheRemove() { _fileCacheMetrics.removeCount.inc(); }

inline int64_t StorageMetrics::GetFileLength(FSMetricGroup metricGroup, FSFileType type) const
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    return _metricsItems[metricGroup][type].fileLength.getValue();
}

inline int64_t StorageMetrics::GetFileCount(FSMetricGroup metricGroup, FSFileType type) const
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    assert(metricGroup >= 0 && metricGroup < FSMG_UNKNOWN);
    return _metricsItems[metricGroup][type].fileCount.getValue();
}

inline int64_t StorageMetrics::GetInMemFileLength(FSMetricGroup metricGroup) const
{
    return GetFileLength(metricGroup, FSFT_MEM);
}

inline int64_t StorageMetrics::GetMmapLockFileLength(FSMetricGroup metricGroup) const
{
    return GetFileLength(metricGroup, FSFT_MMAP_LOCK);
}

inline int64_t StorageMetrics::GetMmapNonlockFileLength(FSMetricGroup metricGroup) const
{
    return GetFileLength(metricGroup, FSFT_MMAP);
}

inline int64_t StorageMetrics::GetResourceFileLength(FSMetricGroup metricGroup) const
{
    return GetFileLength(metricGroup, FSFT_RESOURCE);
}

inline void StorageMetrics::IncreasePackageFileCount() { _packageFileCount.inc(); }

inline void StorageMetrics::DecreasePackageFileCount() { _packageFileCount.dec(); }

inline void StorageMetrics::IncreasePackageInnerFileCount() { _packageInnerFileCount.inc(); }

inline void StorageMetrics::DecreasePackageInnerFileCount() { _packageInnerFileCount.dec(); }

inline void StorageMetrics::IncreasePackageInnerDirCount() { _packageInnerDirCount.inc(); }

inline void StorageMetrics::DecreasePackageInnerDirCount() { _packageInnerDirCount.dec(); }

inline void StorageMetrics::IncreaseFlushMemoryUse(size_t fileLength) { _flushMemoryUse.add(fileLength); }

inline void StorageMetrics::DecreaseFlushMemoryUse(size_t fileLength) { _flushMemoryUse.sub(fileLength); }

typedef std::shared_ptr<StorageMetrics> StorageMetricsPtr;
}} // namespace indexlib::file_system
