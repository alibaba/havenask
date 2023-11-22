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
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "ha3/isearch.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermMetaInfo.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class DocValueFilter;
} // namespace index
} // namespace indexlib

namespace isearch {
namespace search {

class ExecutorVisitor;
class QueryExecutor;

typedef std::vector<QueryExecutor *> SingleLayerExecutors;
typedef std::vector<QueryExecutor *> SingleLayerSeekExecutors;

class QueryExecutor {
protected:
    static constexpr indexlib::index::ErrorCode IE_OK = indexlib::index::ErrorCode::OK;

public:
    class DFCompare {
    public:
        DFCompare() {}

    public:
        bool operator()(QueryExecutor *lft, QueryExecutor *rht) {
            return lft->getCurrentDF() < rht->getCurrentDF();
        }
    };

public:
    QueryExecutor() {
        _current = INVALID_DOCID;
        _currentSub = INVALID_DOCID;
        _seekDocCount = 0;
        _hasSubDocExecutor = true;
        _isEmpty = false;
    }

    virtual ~QueryExecutor() {}

protected:
    enum GetDFType {
        GDT_MAIN_CHAIN,
        GDT_CURRENT_CHAIN
    };

public:
    virtual const std::string getName() const = 0;
    virtual void accept(ExecutorVisitor *visitor) const {};
    virtual indexlib::index::DocValueFilter *stealFilter() {
        return NULL;
    }
    /**
     * seek starting from the *NEXT* docid in the posting list to find the
     * first docid which is not less than id. If there is no such a docid,
     * INVALID_DOCID will be returned.
     *
     * @param id the document id  to be found
     */
    indexlib::index::ErrorCode seek(docid_t id, docid_t &result) {
        if (id > _current) {
            auto ec = doSeek(id, _current);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
        result = _current;
        return IE_OK;
    }

    indexlib::index::ErrorCode seekWithoutCheck(docid_t id, docid_t &result) {
        assert(id > _current);
        auto ret = doSeek(id, _current);
        result = _current;
        return ret;
    }
    indexlib::index::ErrorCode seekSub(docid_t docId,
                                       docid_t subDocId,
                                       docid_t subDocEnd,
                                       bool needSubMatchdata,
                                       docid_t &result) {
        auto ec = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, _currentSub);
        result = _currentSub;
        return ec;
    }

    virtual indexlib::index::ErrorCode seekSubDoc(
        docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t &result)
        = 0;
    virtual void reset() {
        if (_isEmpty) {
            moveToEnd();
        } else {
            _current = INVALID_DOCID;
        }
    }
    virtual bool isMainDocHit(docid_t docId) const = 0;
    bool hasSubDocExecutor() {
        return _hasSubDocExecutor;
    }
    virtual df_t getDF(GetDFType type) const = 0;
    virtual void moveToEnd() {
        _current = END_DOCID;
    }
    virtual void setEmpty() {
        _isEmpty = true;
        moveToEnd();
    }
    virtual void setCurrSub(docid_t docid) {
        _currentSub = docid;
    }
    virtual uint32_t getSeekDocCount() {
        return _seekDocCount;
    }

public:
    // only for test
    virtual std::string toString() const = 0;

public:
    docid_t legacySeek(docid_t id) {
        docid_t ret = INVALID_DOCID;
        auto ec = seek(id, ret);
        indexlib::index::ThrowIfError(ec);
        return ret;
    }
    docid_t legacySeekSub(docid_t docId,
                          docid_t subDocId,
                          docid_t subDocEnd,
                          bool needSubMatchdata = false) {
        docid_t ret = INVALID_DOCID;
        auto ec = seekSub(docId, subDocId, subDocEnd, needSubMatchdata, ret);
        indexlib::index::ThrowIfError(ec);
        return ret;
    }

public:
    inline docid_t getDocId() const {
        return _current;
    }
    inline docid_t getSubDocId() const {
        return _currentSub;
    }
    inline void setDocId(docid_t docid) {
        _current = docid;
    }
    inline df_t getCurrentDF() const {
        return getDF(GDT_CURRENT_CHAIN);
    }
    inline df_t getMainChainDF() const {
        return getDF(GDT_MAIN_CHAIN);
    }
    inline bool isEmpty() const {
        return _isEmpty || _current == END_DOCID;
    }
    inline void setQueryLabel(const std::string &label) {
        _queryLabel = label;
    }
    inline const std::string &getQueryLabel() {
        return _queryLabel;
    }

    /**
     * seek next sub doc id in specified doc.
     * @param docId, specified doc to seek sub doc.
     * @param subDocId, sub docid to seek
     * @param subDocEnd, [subDocBegin, subDocEnd), for AndNotQueryExecutor.
     */
    inline docid_t
    seekSubDoc(docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata) {
        docid_t tmpid = INVALID_DOCID;
        auto ec = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, tmpid);
        indexlib::index::ThrowIfError(ec);
        return tmpid;
    }

private:
    virtual indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) = 0;

public:
    virtual indexlib::index::ErrorCode unpackMatchData(rank::TermMatchData &tmd) {
        tmd.setMatched(true);
        return IE_OK;
    }
    virtual void getMetaInfo(rank::MetaInfo *metaInfo) const {
        rank::TermMetaInfo termMetaInfo;
        termMetaInfo.setMatchDataLevel(MDL_SUB_QUERY);
        termMetaInfo.setQueryLabel(_queryLabel);
        metaInfo->pushBack(termMetaInfo);
    }

    virtual indexlib::matchvalue_t getMatchValue() {
        return indexlib::matchvalue_t();
    }

protected:
    docid_t _current;
    docid_t _currentSub;
    uint32_t _seekDocCount;
    bool _hasSubDocExecutor;
    bool _isEmpty;
    std::string _queryLabel;
};

typedef std::shared_ptr<QueryExecutor> QueryExecutorPtr;

} // namespace search
} // namespace isearch
