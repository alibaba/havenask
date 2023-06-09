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

#include <stdint.h>
#include <string>
#include <vector>

#include "suez/turing/expression/provider/ScoringProvider.h"

#include "ha3/index/SectionReaderWrapper.h"
#include "ha3/config/IndexInfoHelper.h"
#include "ha3/search/ProviderBase.h"
#include "ha3/search/RankResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class FieldBoostTable;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace indexlib { namespace index {
class InvertedIndexReader;
typedef std::shared_ptr<InvertedIndexReader> InvertedIndexReaderPtr;
}}

namespace isearch {
namespace rank {

class ScoringProvider : public suez::turing::ScoringProvider,
                        public search::ProviderBase
{
public:
    typedef std::vector<suez::turing::AttributeExpression*> AttributeVector;
public:
    ScoringProvider(const search::RankResource &rankResource);
    ~ScoringProvider();
public:
    template<typename T>
    T *declareGlobalVariable(const std::string &variName,
                             bool needSerizlize = false);
    const config::IndexInfoHelper *getIndexInfoHelper() const {
        return _rankResource.indexInfoHelper;
    }
    const suez::turing::FieldBoostTable *getFieldBoostTable() const {
        return _rankResource.boostTable;
    }
    void setFieldBoostTable(const suez::turing::FieldBoostTable *fieldBoostTable) {
        _rankResource.boostTable = fieldBoostTable;
    }
    void setIndexReader(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReaderPtr) {
        _rankResource.indexReaderPtr = indexReaderPtr;
    }
    indexlib::index::SectionReaderWrapperPtr getSectionReader(const std::string &indexName = "") const;
    void setTotalMatchDocs(uint32_t v) { _totalMatchDocs = v; }
    uint32_t getTotalMatchDocs() const { return _totalMatchDocs; }
    suez::turing::SuezCavaAllocator *getCavaAllocator() {
        return _rankResource.cavaAllocator;
    }
private:
    search::RankResource _rankResource;
    uint32_t _totalMatchDocs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScoringProvider> ScoringProviderPtr;

template<typename T>
T *ScoringProvider::declareGlobalVariable(const std::string &variName,
        bool needSerizlize)
{
    _declareVariable = true;
    return doDeclareGlobalVariable<T>(variName, needSerizlize);
}

} // namespace rank
} // namespace isearch
