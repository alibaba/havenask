#ifndef __INDEXLIB_STORAGE_METRICS_H
#define __INDEXLIB_STORAGE_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/atomic_counter.h"
#include "indexlib/file_system/file_system_define.h"

IE_NAMESPACE_BEGIN(file_system);

struct StorageMetricsItem
{
    util::AtomicCounter64 fileLength;
    util::AtomicCounter64 fileCount;
};
typedef std::vector<StorageMetricsItem> StorageMetricsItemVec;

struct FileCacheMetrics
{
    util::AtomicCounter64 hitCount;
    util::AtomicCounter64 missCount;
    util::AtomicCounter64 replaceCount;
    util::AtomicCounter64 removeCount;
};

class StorageMetrics
{
public:
    StorageMetrics();
    ~StorageMetrics();

public:
    void IncreaseFile(FSFileType type, int64_t fileLength);
    void DecreaseFile(FSFileType type, int64_t fileLength);

    void IncreaseFileCount(FSFileType type);
    void DecreaseFileCount(FSFileType type);
    void IncreaseFileLength(FSFileType type, int64_t fileLength);
    void DecreaseFileLength(FSFileType type, int64_t fileLength);

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

    int64_t GetFileLength(FSFileType type) const;
    int64_t GetFileCount(FSFileType type) const;
    int64_t GetInMemFileLength() const;
    int64_t GetMmapLockFileLength() const;
    int64_t GetMmapNonlockFileLength() const;
    int64_t GetTotalFileLength() const { return mTotalFileLength.getValue(); }
    int64_t GetTotalFileCount() const { return mTotalFileCount.getValue(); }
    int64_t GetPackageFileCount() const { return mPackageFileCount.getValue(); }
    int64_t GetPackageInnerFileCount() const 
    { return mPackageInnerFileCount.getValue(); }
    int64_t GetPackageInnerDirCount() const 
    { return mPackageInnerDirCount.getValue(); }
    int64_t GetFlushMemoryUse() const 
    { return mFlushMemoryUse.getValue(); }

    const FileCacheMetrics& GetFileCacheMetrics() const
    { return mFileCacheMetrics; }

private:
    StorageMetricsItemVec mMetricsItemVec;    // the index is FSFileType
    FileCacheMetrics mFileCacheMetrics;
    util::AtomicCounter64 mTotalFileLength;
    util::AtomicCounter64 mTotalFileCount;
    util::AtomicCounter64 mPackageFileCount;
    util::AtomicCounter64 mPackageInnerFileCount;
    util::AtomicCounter64 mPackageInnerDirCount;
    util::AtomicCounter64 mFlushMemoryUse;
    
private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////
inline void StorageMetrics::IncreaseFile(FSFileType type, int64_t fileLength)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    IncreaseFileCount(type);
    IncreaseFileLength(type, fileLength);
}

inline void StorageMetrics::DecreaseFile(FSFileType type, int64_t fileLength)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    DecreaseFileCount(type);
    DecreaseFileLength(type, fileLength);
}


inline void StorageMetrics::IncreaseFileCount(FSFileType type)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    mMetricsItemVec[type].fileCount.inc();
    mTotalFileCount.inc();
}

inline void StorageMetrics::DecreaseFileCount(FSFileType type)
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    mMetricsItemVec[type].fileCount.dec();
    mTotalFileCount.dec();
}

inline void StorageMetrics::IncreaseFileLength(FSFileType type,
        int64_t fileLength)
{
    mMetricsItemVec[type].fileLength.add(fileLength);
    mTotalFileLength.add(fileLength);
}

inline void StorageMetrics::DecreaseFileLength(FSFileType type,
        int64_t fileLength)
{
    mMetricsItemVec[type].fileLength.sub(fileLength);
    mTotalFileLength.sub(fileLength);
}

inline void StorageMetrics::IncreaseFileCacheHit()
{
    mFileCacheMetrics.hitCount.inc();
}

inline void StorageMetrics::IncreaseFileCacheMiss()
{
    mFileCacheMetrics.missCount.inc();
}

inline void StorageMetrics::IncreaseFileCacheReplace()
{
    mFileCacheMetrics.replaceCount.inc();
}

inline void StorageMetrics::IncreaseFileCacheRemove()
{
    mFileCacheMetrics.removeCount.inc();
}

inline int64_t StorageMetrics::GetFileLength(FSFileType type) const 
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    return mMetricsItemVec[type].fileLength.getValue();
}

inline int64_t StorageMetrics::GetFileCount(FSFileType type) const 
{
    assert(type >= 0 && type < FSFT_UNKNOWN);
    return mMetricsItemVec[type].fileCount.getValue();
}

inline int64_t StorageMetrics::GetInMemFileLength() const
{
    return GetFileLength(FSFT_IN_MEM);
}

inline int64_t StorageMetrics::GetMmapLockFileLength() const
{
    return GetFileLength(FSFT_MMAP_LOCK);
}

inline int64_t StorageMetrics::GetMmapNonlockFileLength() const
{
    return GetFileLength(FSFT_MMAP);
}

inline void StorageMetrics::IncreasePackageFileCount()
{
    mPackageFileCount.inc();
}

inline void StorageMetrics::DecreasePackageFileCount()
{
    mPackageFileCount.dec();
}

inline void StorageMetrics::IncreasePackageInnerFileCount()
{
    mPackageInnerFileCount.inc();
}

inline void StorageMetrics::DecreasePackageInnerFileCount()
{
    mPackageInnerFileCount.dec();
}

inline void StorageMetrics::IncreasePackageInnerDirCount()
{
    mPackageInnerDirCount.inc();
}

inline void StorageMetrics::DecreasePackageInnerDirCount()
{
    mPackageInnerDirCount.dec();
}

inline void StorageMetrics::IncreaseFlushMemoryUse(size_t fileLength)
{
    mFlushMemoryUse.add(fileLength);
}

inline void StorageMetrics::DecreaseFlushMemoryUse(size_t fileLength)
{
    mFlushMemoryUse.sub(fileLength);
}

DEFINE_SHARED_PTR(StorageMetrics);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_STORAGE_METRICS_H
