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

#include "indexlib/misc/common.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/provider/MatchInfoReader.h"
#include "suez/turing/expression/provider/ProviderBase.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class InvertedIndexReader;
typedef std::shared_ptr<InvertedIndexReader> InvertedIndexReaderPtr;
class SectionReaderWrapper;
typedef std::shared_ptr<SectionReaderWrapper> SectionReaderWrapperPtr;
} // namespace index
} // namespace indexlib
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {
class MetaInfo;
class SuezCavaAllocator;
class Tracer;
class IndexInfoHelper;
class FieldMetaReaderWrapper;

class FunctionProvider : public ProviderBase {
public:
    FunctionProvider(matchdoc::MatchDocAllocator *allocator,
                     autil::mem_pool::Pool *pool,
                     SuezCavaAllocator *cavaAllocator,
                     Tracer *requestTracer,
                     indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                     const KeyValueMap *kvpairs,
                     kmonitor::MetricsReporter *metricsReporter = nullptr);
    virtual ~FunctionProvider();

private:
    FunctionProvider(const FunctionProvider &);
    FunctionProvider &operator=(const FunctionProvider &);

public:
    // only for test compatibility, DO NOT USE
    FunctionProvider(matchdoc::MatchDocAllocator *allocator,
                     autil::mem_pool::Pool *pool,
                     Tracer *requestTracer,
                     indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                     const KeyValueMap *kvpairs)
        : FunctionProvider(allocator, pool, nullptr, requestTracer, partitionReaderSnapshot, kvpairs) {}
    MatchInfoReader *getMatchInfoReader() {
        if (_matchInfoReader.getMetaInfo() == nullptr) {
            return nullptr;
        }
        return &_matchInfoReader;
    }
    void initMatchInfoReader(std::shared_ptr<MetaInfo> metaInfo);

    void setIndexInfoHelper(const IndexInfoHelper *indexInfoHelper) { _indexInfoHelper = indexInfoHelper; }

    const IndexInfoHelper *getIndexInfoHelper() const { return _indexInfoHelper; }

    void setIndexReaderPtr(std::shared_ptr<indexlib::index::InvertedIndexReader> indexReaderPtr) {
        _indexReaderPtr = indexReaderPtr;
    }

    std::shared_ptr<indexlib::index::InvertedIndexReader> getIndexReaderPtr() const { return _indexReaderPtr; }

    void setFieldMetaReaderWrapper(std::shared_ptr<suez::turing::FieldMetaReaderWrapper> fieldMetaReaderWrapperPtr) {_fieldMetaReaderWrapperPtr = fieldMetaReaderWrapperPtr;}
    std::shared_ptr<suez::turing::FieldMetaReaderWrapper> getFieldMetaReaderWrapper() {return _fieldMetaReaderWrapperPtr;}

    indexlib::index::SectionReaderWrapperPtr getSectionReader(const std::string &indexName = "") const;

    void setDebug(bool debug) { _debug = debug; }

    bool isDebug() { return _debug; }

private:
    MatchInfoReader _matchInfoReader;
    const IndexInfoHelper *_indexInfoHelper;
    std::shared_ptr<indexlib::index::InvertedIndexReader> _indexReaderPtr;
    std::shared_ptr<suez::turing::FieldMetaReaderWrapper> _fieldMetaReaderWrapperPtr;
    bool _debug = false;
};

SUEZ_TYPEDEF_PTR(FunctionProvider);

} // namespace turing
} // namespace suez
