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
#include <memory>
#include <string>

#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class SubDocIdTermQueryExecutor : public TermQueryExecutor
{
public:
    SubDocIdTermQueryExecutor(docid_t docId, docid_t subDocId)
        : TermQueryExecutor(nullptr, {})
        , _docId(docId)
        , _subDocId(subDocId)
    {
        _hasSubDocExecutor = true;
    }
    virtual ~SubDocIdTermQueryExecutor() {}
private:
    SubDocIdTermQueryExecutor(const DocIdTermQueryExecutor &);
    SubDocIdTermQueryExecutor& operator=(const DocIdTermQueryExecutor &);
public:
    const std::string getName() const override {
        return "SubDocIdTermQueryExecutor";
    }

    void reset() override {}

    bool isMainDocHit(docid_t docId) const override {
        return false;
    }
protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t& result) override {
        result = _docId >= id ? _docId : END_DOCID;
        return indexlib::index::ErrorCode::OK;
    }

    indexlib::index::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override
    {
        if (docId == _docId && subDocId <= _subDocId) {
            result = _subDocId;
        } else {
            result = END_DOCID;
        }
        return indexlib::index::ErrorCode::OK;
    }
private:
    docid_t _docId;
    docid_t _subDocId;
};
typedef std::shared_ptr<SubDocIdTermQueryExecutor> SubDocIdTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
