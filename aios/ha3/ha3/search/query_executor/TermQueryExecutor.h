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
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/TermMatchData.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {
class IndexPartitionReaderWrapper;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {
class ExecutorVisitor;

class TermQueryExecutor : public QueryExecutor {
public:
    TermQueryExecutor(indexlib::index::PostingIterator *iter, const common::Term &term);
    virtual ~TermQueryExecutor();

private:
    TermQueryExecutor(const TermQueryExecutor &);
    TermQueryExecutor &operator=(const TermQueryExecutor &);

public:
    const std::string getName() const override {
        return "TermQueryExecutor";
    }
    void accept(ExecutorVisitor *visitor) const override;
    df_t getDF(GetDFType type) const override;
    void reset() override;

    size_t getLeafId() const {
        return _LeafId;
    }

    void setLeafId(size_t id) {
        _LeafId = id;
    }

    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override;

    bool isMainDocHit(docid_t docId) const override {
        return true;
    }

    indexlib::index::ErrorCode seekPosition(pos_t pos, pos_t &result) {
        assert(_iter);
        return _iter->SeekPositionWithErrorCode(pos, result);
    }

    bool hasPosition() const {
        if (_iter) {
            return _iter->HasPosition();
        }
        return false;
    }

    std::string getIndexName() const {
        return _term.getIndexName();
    }

    void setMainChainDF(df_t df) {
        _mainChainDF = df;
    }

    void
    setIndexPartitionReaderWrapper(isearch::search::IndexPartitionReaderWrapper *indexPartWrapper) {
        _indexPartReaderWrapper = indexPartWrapper;
    }

    indexlib::index::PostingIterator *getPostingIterator() {
        return _iter;
    }

public:
    indexlib::index::ErrorCode unpackMatchData(rank::TermMatchData &tmd) override {
        assert(_iter);
        return _iter->UnpackWithErrorCode(tmd.getTermMatchData());
    }
    void getMetaInfo(rank::MetaInfo *metaInfo) const override;

    indexlib::matchvalue_t getMatchValue() override {
        return _iter->GetMatchValue();
    }

public:
    std::string toString() const override;

protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

protected:
    indexlib::index::PostingIterator *_iter = nullptr;
    indexlib::index::PostingIterator *_backupIter = nullptr;
    common::Term _term;
    df_t _mainChainDF;
    size_t _LeafId;

private:
    isearch::search::IndexPartitionReaderWrapper *_indexPartReaderWrapper = nullptr;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<TermQueryExecutor> TermQueryExecutorPtr;

} // namespace search
} // namespace isearch
