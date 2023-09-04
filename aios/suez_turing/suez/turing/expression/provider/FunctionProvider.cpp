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
#include "suez/turing/expression/provider/FunctionProvider.h"

#include <iosfwd>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "suez/turing/expression/util/SectionReaderWrapper.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {
class SectionAttributeReader;
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
class SuezCavaAllocator;
class Tracer;
} // namespace turing
} // namespace suez

using namespace std;
using namespace indexlib::partition;
using namespace indexlib::index;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, FunctionProvider);

namespace suez {
namespace turing {

FunctionProvider::FunctionProvider(matchdoc::MatchDocAllocator *allocator,
                                   autil::mem_pool::Pool *pool,
                                   SuezCavaAllocator *cavaAllocator,
                                   Tracer *requestTracer,
                                   PartitionReaderSnapshot *partitionReaderSnapshot,
                                   const KeyValueMap *kvpairs,
                                   kmonitor::MetricsReporter *metricsReporter)
    : ProviderBase(allocator, pool, cavaAllocator, requestTracer, partitionReaderSnapshot, kvpairs, metricsReporter)
    , _indexInfoHelper(nullptr)
    , _debug(false) {}

FunctionProvider::~FunctionProvider() {}

void FunctionProvider::initMatchInfoReader(std::shared_ptr<MetaInfo> metaInfo) {
    _matchInfoReader.init(std::move(metaInfo), _allocator);
}

SectionReaderWrapperPtr FunctionProvider::getSectionReader(const string &indexName) const {
    if (!_indexReaderPtr) {
        return SectionReaderWrapperPtr();
    }

    const SectionAttributeReader *attrReader = _indexReaderPtr->GetSectionReader(indexName);
    if (nullptr == attrReader) {
        return SectionReaderWrapperPtr();
    }
    SectionReaderWrapperPtr ptr(new SectionReaderWrapper(attrReader));
    return ptr;
}

} // namespace turing
} // namespace suez
