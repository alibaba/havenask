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

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/misc/common.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

namespace indexlib::index {
class DeletionMapReaderAdaptor;
}

namespace indexlib {
namespace index {
class JoinDocidAttributeIterator;
}  // namespace index
}  // namespace indexlib
namespace isearch {
namespace search {
class MatchDataManager;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace turing {

struct SeekIteratorParam {
    SeekIteratorParam()
        : layerMeta(NULL)
        , timeoutTerminator(NULL)
        , matchDataManager(NULL)
        , needAllSubDocFlag(false)
    {
    }
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    search::QueryExecutorPtr queryExecutor;
    search::FilterWrapperPtr filterWrapper;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    indexlib::index::DeletionMapReaderPtr subDelMapReader;
    search::LayerMeta *layerMeta;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt;
    common::TimeoutTerminator *timeoutTerminator;
    search::MatchDataManager *matchDataManager;
    bool needAllSubDocFlag;
};


class SeekIterator {
public:
    SeekIterator(const SeekIteratorParam &param) {
        _indexPartitionReaderWrapper = param.indexPartitionReaderWrapper;
        _queryExecutor = param.queryExecutor;
        _layerMeta = param.layerMeta;
        _filterWrapper = param.filterWrapper;
        _matchDocAllocator = param.matchDocAllocator;
        _delMapReader = param.delMapReader;
        _subDelMapReader = param.subDelMapReader;
        _needSubDoc = param.matchDocAllocator->hasSubDocAllocator();
        _singleLayerSearcher.reset(new search::SingleLayerSearcher(
                        _queryExecutor.get(), _layerMeta, _filterWrapper.get(), _delMapReader.get(),
                        _matchDocAllocator.get(), param.timeoutTerminator,
                        param.mainToSubIt, _subDelMapReader.get(), param.matchDataManager,
                        param.needAllSubDocFlag));
    }

    uint32_t getSeekTimes() const {
        return _singleLayerSearcher->getSeekTimes();
    }

    uint32_t getSeekDocCount() const {
        return _singleLayerSearcher->getSeekDocCount();
    }

    matchdoc::MatchDoc seek() {
        matchdoc::MatchDoc matchDoc;
        auto ec = _singleLayerSearcher->seek(_needSubDoc, matchDoc);
        if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
            if (ec == indexlib::index::ErrorCode::FileIO) {
                return matchdoc::INVALID_MATCHDOC;
            }
            indexlib::index::ThrowIfError(ec);
        }
        return matchDoc;
    }


    bool batchSeek(int32_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
        int32_t docCount = 0;
        while (true) {
            matchdoc::MatchDoc doc = seek();
            if (matchdoc::INVALID_MATCHDOC == doc) {
                return true;
            }
            matchDocs.push_back(doc);
            docCount++;
            if (docCount >= batchSize) {
                return false;
            }
        }
    }

    matchdoc::MatchDocAllocatorPtr getMatchDocAllocator() const {
        return _matchDocAllocator;
    }

private:
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    search::FilterWrapperPtr _filterWrapper;
    search::QueryExecutorPtr _queryExecutor;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> _delMapReader;
    indexlib::index::DeletionMapReaderPtr _subDelMapReader;
    search::LayerMeta *_layerMeta;
    search::SingleLayerSearcherPtr _singleLayerSearcher;
    bool _needSubDoc;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SeekIterator> SeekIteratorPtr;

class SeekIteratorVariant
{
public:
    SeekIteratorVariant() {}
    SeekIteratorVariant(const SeekIteratorPtr &seekIterator,
                        const search::LayerMetasPtr &layerMetas,
                        const ExpressionResourcePtr &resource)
        : _seekIterator(seekIterator)
        , _layerMetas(layerMetas)
        , _expressionResource(resource)
    {
    }
    ~SeekIteratorVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "SeekIterator";
    }
public:
    SeekIteratorPtr getSeekIterator() const {
        return _seekIterator;
    }
private:
    SeekIteratorPtr _seekIterator;
    search::LayerMetasPtr _layerMetas;
    ExpressionResourcePtr _expressionResource;
private:
    AUTIL_LOG_DECLARE();

};

typedef std::shared_ptr<SeekIteratorVariant> SeekIteratorVariantPtr;

} // namespace turing
} // namespace isearch
