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
#include "ha3/search/TermQueryExecutor.h"

#include <stddef.h>

#include "alog/Logger.h"
#include "ha3/search/ExecutorVisitor.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/TermMetaInfo.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

using namespace indexlib::index;
using namespace isearch::common;
namespace isearch {
namespace search {

AUTIL_LOG_SETUP(ha3, TermQueryExecutor);

TermQueryExecutor::TermQueryExecutor(PostingIterator *iter, const common::Term &term)
    : QueryExecutor()
    , _iter(iter)
    , _backupIter(NULL)
    , _term(term)
    , _LeafId(0)
    , _indexPartReaderWrapper(NULL) {
    if (_iter) {
        _mainChainDF = _iter->GetTermMeta()->GetDocFreq();
    }
    _hasSubDocExecutor = false;
}

TermQueryExecutor::~TermQueryExecutor() {}

void TermQueryExecutor::accept(ExecutorVisitor *visitor) const {
    visitor->visitTermExecutor(this);
}

indexlib::index::ErrorCode TermQueryExecutor::doSeek(docid_t id, docid_t &result) {
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _iter->SeekDocWithErrorCode(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    ++_seekDocCount;
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode TermQueryExecutor::seekSubDoc(
    docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t &result) {
    assert(getDocId() >= docId);
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
    } else {
        result = END_DOCID;
    }
    return indexlib::index::ErrorCode::OK;
}

void TermQueryExecutor::getMetaInfo(rank::MetaInfo *metaInfo) const {
    rank::TermMetaInfo termMetaInfo;
    termMetaInfo.setMatchDataLevel(MDL_TERM);
    termMetaInfo.setQueryLabel(_queryLabel);
    termMetaInfo.setTermInfo(
        _term.getToken(), _term.getIndexName(), _term.getBoost(), _term.getTruncateName());
    if (_iter) {
        termMetaInfo.setTermMeta(_iter->GetTermMeta());
    }
    AUTIL_LOG(TRACE3, "term_boost: %d", termMetaInfo.getTermBoost());
    metaInfo->pushBack(termMetaInfo);
}

df_t TermQueryExecutor::getDF(GetDFType type) const {
    if (!_iter) {
        return df_t(0);
    }
    if (type == GDT_MAIN_CHAIN) {
        return _iter->GetTermMeta()->GetDocFreq();
    } else {
        assert(type == GDT_CURRENT_CHAIN);
        return _iter->GetTruncateTermMeta()->GetDocFreq();
    }
}

void TermQueryExecutor::reset() {
    QueryExecutor::reset();
    //    assert(!_backupIter);
    _backupIter = _iter;
    if (_backupIter) {
        _iter = _backupIter->Clone();
        assert(_indexPartReaderWrapper != NULL);
        _indexPartReaderWrapper->addDelPosting(_term.getIndexName(), _iter);
    }
}

std::string TermQueryExecutor::toString() const {
    return _term.getWord().c_str();
}

} // namespace search
} // namespace isearch
