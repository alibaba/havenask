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

class DocIdTermQueryExecutor : public TermQueryExecutor {
public:
    DocIdTermQueryExecutor(docid_t docId)
        : TermQueryExecutor(nullptr, {})
        , _docId(docId) {}
    virtual ~DocIdTermQueryExecutor() {}

private:
    DocIdTermQueryExecutor(const DocIdTermQueryExecutor &);
    DocIdTermQueryExecutor &operator=(const DocIdTermQueryExecutor &);

public:
    const std::string getName() const override {
        return "DocIdTermQueryExecutor";
    }

    void reset() override {}

protected:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override {
        result = _docId >= id ? _docId : END_DOCID;
        return indexlib::index::ErrorCode::OK;
    }

private:
    docid_t _docId;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<DocIdTermQueryExecutor> DocIdTermQueryExecutorPtr;

} // namespace search
} // namespace isearch
