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
#include "suez/turing/expression/cava/impl/MatchInfoReader.h"

#include <iosfwd>
#include <string>

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "cava/runtime/CavaCtx.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "suez/turing/expression/provider/MatchValues.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/provider/SimpleMatchData.h"
#include "suez/turing/expression/provider/TermMetaInfo.h"

namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava
namespace ha3 {
class MatchValue;
class MatchValues;
class SimpleMatchData;
} // namespace ha3
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

using namespace std;
using namespace matchdoc;

namespace ha3 {

MatchInfoReader::MatchInfoReader()
    : _inited(false), _metaInfo(nullptr), _simpleMatchDataRef(nullptr), _matchValuesRef(nullptr) {}

MatchInfoReader::~MatchInfoReader() {
    _inited = false;
    _metaInfo = nullptr;
    _simpleMatchDataRef = nullptr;
    _matchValuesRef = nullptr;
}

void MatchInfoReader::init(suez::turing::MetaInfo *metaInfo,
                           const Reference<suez::turing::SimpleMatchData> *simpleMatchDataRef) {
    _inited = true;
    _metaInfo = metaInfo;
    _simpleMatchDataRef = simpleMatchDataRef;
}

void MatchInfoReader::setMatchValuesRef(const Reference<suez::turing::MatchValues> *matchValuesRef) {
    _matchValuesRef = matchValuesRef;
}

uint MatchInfoReader::getQuerySize(CavaCtx *ctx) {
    if (_metaInfo == nullptr) {
        ctx->setOtherException("getQuerySize exception: meta info is null");
        return 0;
    }
    return _metaInfo->size();
}

int32_t MatchInfoReader::getDocFreq(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getDocFreq exception: meta info size < " + autil::StringUtil::toString(idx));
        return 0;
    }
    return (*_metaInfo)[idx].getDocFreq();
}

int32_t MatchInfoReader::getTotalTermFreq(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getTotalTermFreq exception: meta info size < " + autil::StringUtil::toString(idx));
        return 0;
    }
    return (*_metaInfo)[idx].getTotalTermFreq();
}

uint32_t MatchInfoReader::getPayload(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getPayload exception: meta info size < " + autil::StringUtil::toString(idx));
        return 0;
    }
    return (*_metaInfo)[idx].getPayload();
}

int32_t MatchInfoReader::getTermBoost(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getTermBoost exception: meta info size < " + autil::StringUtil::toString(idx));
        return 0;
    }
    return (*_metaInfo)[idx].getTermBoost();
}

cava::lang::CString *MatchInfoReader::getIndexName(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getIndexName exception: meta info size < " + autil::StringUtil::toString(idx));
        return nullptr;
    }
    const string &indexNameStr = (*_metaInfo)[idx].getIndexName();
    const auto temConstString = autil::StringView(indexNameStr);
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, &temConstString);
}

cava::lang::CString *MatchInfoReader::getTermText(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getTermText exception: meta info size < " + autil::StringUtil::toString(idx));
        return nullptr;
    }
    const string &termTextStr = (*_metaInfo)[idx].getTermText();
    const auto temConstString = autil::StringView(termTextStr);
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, &temConstString);
}

cava::lang::CString *MatchInfoReader::getQueryLabel(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getQueryLabel exception: meta info size < " + autil::StringUtil::toString(idx));
        return nullptr;
    }

    const string &queryLabelStr = (*_metaInfo)[idx].getQueryLabel();
    const auto &queryLabelConstString = autil::StringView(queryLabelStr);
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, &queryLabelConstString);
}

int32_t MatchInfoReader::getMatchDataLevel(CavaCtx *ctx, uint idx) {
    if (nullptr == _metaInfo || idx >= _metaInfo->size()) {
        ctx->setOtherException("getMatchDataLevel exception: meta info size < " + autil::StringUtil::toString(idx));
        return -1;
    }
    return (int32_t)(*_metaInfo)[idx].getMatchDataLevel();
}

ha3::SimpleMatchData *MatchInfoReader::getSimpleMatchData(CavaCtx *ctx, MatchDoc *doc) {
    if (nullptr == _metaInfo || nullptr == _simpleMatchDataRef) {
        return nullptr;
    }

    const auto *simpleMatchData = _simpleMatchDataRef->getPointer(*(matchdoc::MatchDoc *)doc);
    return (ha3::SimpleMatchData *)simpleMatchData;
}

ha3::MatchValues *MatchInfoReader::getMatchValues(CavaCtx *ctx, MatchDoc *doc) {
    if (nullptr == _metaInfo || nullptr == _matchValuesRef) {
        return nullptr;
    }

    const auto *matchValues = _matchValuesRef->getPointer(*(matchdoc::MatchDoc *)doc);
    return (ha3::MatchValues *)matchValues;
}

bool MatchInfoReader::isMatch(CavaCtx *ctx, ha3::SimpleMatchData *cavaSimpleMatchData, uint idx) {
    if (nullptr == cavaSimpleMatchData || idx >= getQuerySize(ctx)) {
        ctx->setOtherException("isMatch exception");
        return false;
    }

    suez::turing::SimpleMatchData *simpleMatchData = (suez::turing::SimpleMatchData *)cavaSimpleMatchData;
    return simpleMatchData->isMatch(idx);
}

ha3::MatchValue *MatchInfoReader::getTermMatchValue(CavaCtx *ctx, ha3::MatchValues *cavaMatchValues, uint idx) {
    if (nullptr == cavaMatchValues || idx >= getQuerySize(ctx)) {
        ctx->exception = 1;
        return nullptr;
    }

    suez::turing::MatchValues *matchValues = (suez::turing::MatchValues *)cavaMatchValues;
    return (ha3::MatchValue *)&matchValues->getTermMatchValue(idx);
}

} // namespace ha3
