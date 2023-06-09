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
#ifndef __INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H
#define __INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class BucketVectorAllocator
{
public:
    typedef std::vector<docid_t> DocIdVector;
    typedef std::shared_ptr<DocIdVector> DocIdVectorPtr;
    typedef std::vector<DocIdVector> BucketVector;
    typedef std::shared_ptr<BucketVector> BucketVectorPtr;

public:
    BucketVectorAllocator() {}
    ~BucketVectorAllocator() {}

public:
    BucketVectorPtr AllocateBucket()
    {
        autil::ScopedLock lock(mLock);
        if (mBucketVecs.empty()) {
            return BucketVectorPtr(new BucketVector);
        }
        BucketVectorPtr bucketVec = mBucketVecs.back();
        mBucketVecs.pop_back();
        return bucketVec;
    }
    void DeallocateBucket(const BucketVectorPtr& bucketVec)
    {
        autil::ScopedLock lock(mLock);
        mBucketVecs.push_back(bucketVec);
    }

    DocIdVectorPtr AllocateDocVector()
    {
        autil::ScopedLock lock(mLock);
        if (mDocIdVecs.empty()) {
            return DocIdVectorPtr(new DocIdVector);
        }
        DocIdVectorPtr docIdVec = mDocIdVecs.back();
        mDocIdVecs.pop_back();
        return docIdVec;
    }
    void DeallocateDocIdVec(const DocIdVectorPtr& docIdVec)
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
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BUCKET_VECTOR_ALLOCATOR_H
