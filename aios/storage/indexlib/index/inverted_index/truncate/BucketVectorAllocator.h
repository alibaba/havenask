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

#include <memory>
#include <mutex>
#include <vector>

#include "indexlib/base/Types.h"

namespace indexlib::index {

class BucketVectorAllocator
{
public:
    using DocIdVector = std::vector<docid_t>;
    using BucketVector = std::vector<DocIdVector>;

public:
    BucketVectorAllocator() {}
    ~BucketVectorAllocator() {}

public:
    std::shared_ptr<BucketVector> AllocateBucket()
    {
        std::lock_guard<std::mutex> guard(_mutex);
        if (_bucketVecs.empty()) {
            return std::make_shared<BucketVector>();
        }
        std::shared_ptr<BucketVector> bucketVec = _bucketVecs.back();
        _bucketVecs.pop_back();
        return bucketVec;
    }
    void DeallocateBucket(const std::shared_ptr<BucketVector>& bucketVec)
    {
        std::lock_guard<std::mutex> guard(_mutex);
        _bucketVecs.push_back(bucketVec);
    }

    std::shared_ptr<DocIdVector> AllocateDocVector()
    {
        std::lock_guard<std::mutex> guard(_mutex);
        if (_docIdVecs.empty()) {
            return std::make_shared<DocIdVector>();
        }
        std::shared_ptr<DocIdVector> docIdVec = _docIdVecs.back();
        _docIdVecs.pop_back();
        return docIdVec;
    }
    void DeallocateDocIdVec(const std::shared_ptr<DocIdVector>& docIdVec)
    {
        std::lock_guard<std::mutex> guard(_mutex);
        _docIdVecs.push_back(docIdVec);
    }

private:
    std::mutex _mutex;
    std::vector<std::shared_ptr<BucketVector>> _bucketVecs;
    std::vector<std::shared_ptr<DocIdVector>> _docIdVecs;
};

} // namespace indexlib::index
