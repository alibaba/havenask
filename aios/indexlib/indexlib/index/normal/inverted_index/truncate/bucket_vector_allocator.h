#ifndef __INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H
#define __INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);


class BucketVectorAllocator
{
public:
    typedef std::vector<docid_t> DocIdVector;
    typedef std::tr1::shared_ptr<DocIdVector> DocIdVectorPtr;
    typedef std::vector<DocIdVector> BucketVector;
    typedef std::tr1::shared_ptr<BucketVector> BucketVectorPtr;
public:
    BucketVectorAllocator() {}
    ~BucketVectorAllocator() {}
public:
    BucketVectorPtr AllocateBucket()
    {
        autil::ScopedLock lock(mLock);
        if (mBucketVecs.empty())
        {
            return BucketVectorPtr(new BucketVector);
        }
        BucketVectorPtr bucketVec = mBucketVecs.back();
        mBucketVecs.pop_back();
        return bucketVec;
    }
    void DeallocateBucket(const BucketVectorPtr &bucketVec)
    {
        autil::ScopedLock lock(mLock);
        mBucketVecs.push_back(bucketVec);
    }

    DocIdVectorPtr AllocateDocVector()
    {
        autil::ScopedLock lock(mLock);
        if (mDocIdVecs.empty())
        {
            return DocIdVectorPtr(new DocIdVector);
        }
        DocIdVectorPtr docIdVec = mDocIdVecs.back();
        mDocIdVecs.pop_back();
        return docIdVec;
    }
    void DeallocateDocIdVec(const DocIdVectorPtr &docIdVec)
    {
        autil::ScopedLock lock(mLock);
        mDocIdVecs.push_back(docIdVec);
    }

private:
    std::vector<BucketVectorPtr> mBucketVecs;
    std::vector<DocIdVectorPtr> mDocIdVecs;
    autil::ThreadMutex mLock;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BucketVectorAllocator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H
