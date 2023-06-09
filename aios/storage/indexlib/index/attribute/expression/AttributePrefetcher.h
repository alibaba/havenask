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
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/memory.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
namespace indexlibv2::config {
class AttributeConfig;
}
namespace indexlibv2::index {

template <typename T>
class AttributePrefetcher : private autil::NoCopyable
{
public:
    typedef index::AttributeIteratorTyped<T> TypedIter;
    AttributePrefetcher() : _currentValue(INVALID_DOCID, T()) {}
    ~AttributePrefetcher() {}

public:
    Status Init(autil::mem_pool::Pool* pool, const std::shared_ptr<config::AttributeConfig>& attrConfig,
                std::vector<std::shared_ptr<framework::Segment>> segments);
    Status Prefetch(docid_t docId);
    T GetValue() const { return _currentValue.second; }
    docid_t GetCurrentDocId() const { return _currentValue.first; }

private:
    std::unique_ptr<index::AttributeReader> _attrReader;
    std::unique_ptr<TypedIter, autil::PoolDeleter<autil::mem_pool::Pool, TypedIter>> _attrIterator;
    std::pair<docid_t, T> _currentValue;
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, AttributePrefetcher, T);
template <typename T>
Status AttributePrefetcher<T>::Init(autil::mem_pool::Pool* pool,
                                    const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                    std::vector<std::shared_ptr<framework::Segment>> segments)
{
    index::AttributeIndexFactory indexFactory;
    index::IndexerParameter indexParam;
    auto indexReader = indexFactory.CreateIndexReader(attrConfig, indexParam).release();
    _attrReader.reset(dynamic_cast<index::AttributeReader*>(indexReader));
    if (!_attrReader) {
        DELETE_AND_SET_NULL(indexReader);
        AUTIL_LOG(ERROR, "create attribute reader failed");
        return Status::InvalidArgs("create attribute reader failed");
    }
    RETURN_IF_STATUS_ERROR(_attrReader->OpenWithSegments(attrConfig, segments), "open attribute reader failed");
    index::AttributeIteratorBase* iterBase = _attrReader->CreateIterator(pool);
    auto iter = dynamic_cast<TypedIter*>(iterBase);
    if (!iter) {
        DELETE_AND_SET_NULL(iterBase);
        AUTIL_LOG(ERROR, "create attribute iterator failed");
        return Status::InvalidArgs("create attribute iterator failed");
    }
    _attrIterator = autil::unique_ptr_pool(pool, iter);
    return Status::OK();
}

template <typename T>
Status AttributePrefetcher<T>::Prefetch(docid_t docId)
{
    if (docId == _currentValue.first) {
        return Status::OK();
    }
    if (docId < _currentValue.first) {
        AUTIL_LOG(ERROR, "doc id rollback from [%d] to [%d], prefetch failed", _currentValue.first, docId);
        return Status::InvalidArgs("docid rollback");
    }
    T value = T();
    bool success = false;
    try {
        success = _attrIterator->Seek(docId, value);
    } catch (...) {
        AUTIL_LOG(ERROR, "prefetch docid [%d] failed", docId);
        return Status::Corruption("prefetch failed,  exception");
    }
    if (!success) {
        AUTIL_LOG(ERROR, "prefetch docid [%d] failed, seek failed ", docId);
        return Status::Corruption("prefetch failed");
    }
    _currentValue = std::make_pair(docId, value);
    return Status::OK();
}

} // namespace indexlibv2::index
