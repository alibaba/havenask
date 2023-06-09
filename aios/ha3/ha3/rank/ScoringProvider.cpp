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
#include "ha3/rank/ScoringProvider.h"

#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "indexlib/misc/common.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/partition/partition_define.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/index/SectionReaderWrapper.h"
#include "ha3/index/index.h"
#include "ha3/search/RankResource.h"
#include "ha3/search/SearchPluginResource.h"
#include "autil/Log.h"

namespace indexlib {
namespace index {
class SectionAttributeReader;
}  // namespace index
}  // namespace indexlib

using namespace std;
using namespace suez::turing;
using namespace isearch::search;
using namespace isearch::common;
using namespace indexlib::index;
using namespace indexlib::inverted_index;
using namespace isearch::config;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, ScoringProvider);

ScoringProvider::ScoringProvider(const RankResource &rankResource)
    : suez::turing::ScoringProvider(rankResource.matchDocAllocator.get(),
                                    rankResource.pool,
                                    rankResource.cavaAllocator,
                                    rankResource.requestTracer,
                                    rankResource.partitionReaderSnapshot,
                                    rankResource.kvpairs)
    , _rankResource(rankResource)
{
    auto creator = dynamic_cast<AttributeExpressionCreator *>(rankResource.attrExprCreator);
    if (creator) {
        setAttributeExpressionCreator(creator);
    }
    _totalMatchDocs = 0;
    setSearchPluginResource(&_rankResource);
    setupTraceRefer(convertRankTraceLevel(getRequest()));
}

ScoringProvider::~ScoringProvider() {
}

SectionReaderWrapperPtr
ScoringProvider::getSectionReader(const string &indexName) const {
    if (NULL == _rankResource.indexReaderPtr.get()) {
        SectionReaderWrapperPtr nullWrapperPtr;
        return nullWrapperPtr;
    }

    const SectionAttributeReader* attrReader
        = _rankResource.indexReaderPtr->GetSectionReader(indexName);
    if (NULL == attrReader) {
        AUTIL_LOG(WARN, "not found the SectionAttributeReader, indexName:%s",
            indexName.c_str());
        SectionReaderWrapperPtr nullWrapperPtr;
        return nullWrapperPtr;
    }
    SectionReaderWrapperPtr ptr(new SectionReaderWrapper(attrReader));
    return ptr;
}

} // namespace rank
} // namespace isearch
