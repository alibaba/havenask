#ifndef __INDEXLIB_BUCKET_MAP_H
#define __INDEXLIB_BUCKET_MAP_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(index);

class BucketMap
{
public:
    static const std::string BUKCET_MAP_FILE_NAME_PREFIX;
public:
    BucketMap();
    ~BucketMap();
public:
    bool Init(uint32_t size);

    void SetSortValue(docid_t docId, uint32_t sortValue);
    uint32_t GetSortValue(docid_t docId);

    uint32_t GetBucketValue(docid_t docId);

    uint32_t GetBucketCount() const { return mBucketCount; }
    uint32_t GetBucketSize() const { return mBucketSize; }
    uint32_t GetSize() const { return mSize; }

    void Store(const std::string &filePath) const;
    void Store(const file_system::DirectoryPtr &directory,
               const std::string &fileName) const;
    bool Load(const std::string &filePath);
    bool Load(const file_system::DirectoryPtr &directory,
              const std::string &fileName);

    int64_t EstimateMemoryUse() const;
private:
    uint32_t GetBucketCount(uint32_t size);
    bool InnerLoad(file_system::FileReaderPtr& reader);

private:
    uint32_t *mSortValues;
    uint32_t mSize;
    uint32_t mBucketCount;
    uint32_t mBucketSize;

private:
    IE_LOG_DECLARE();
};

inline void BucketMap::SetSortValue(docid_t docId, uint32_t sortValue)
{
    assert(mSortValues != NULL);
    assert((uint32_t)docId < mSize);
    assert(sortValue < mSize);

    mSortValues[docId] = sortValue;
}

inline uint32_t BucketMap::GetSortValue(docid_t docId)
{
    assert((uint32_t)docId < mSize);
    assert(mSortValues != NULL);
    return mSortValues[docId];
}

inline uint32_t BucketMap::GetBucketValue(docid_t docId)
{
    assert((uint32_t)docId < mSize);
    return mSortValues[docId] / mBucketSize;
}

DEFINE_SHARED_PTR(BucketMap);
typedef std::map<std::string, BucketMapPtr> BucketMaps;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUCKET_MAP_H
