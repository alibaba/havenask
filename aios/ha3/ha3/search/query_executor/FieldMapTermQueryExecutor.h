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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/BufferedTermQueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class PostingIterator;
}  // namespace index
}  // namespace indexlib
namespace isearch {
namespace common {
class Term;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

enum FieldMatchOperatorType {
    FM_AND,
    FM_OR
};

class FieldMapTermQueryExecutor : public BufferedTermQueryExecutor
{
public:
    FieldMapTermQueryExecutor(indexlib::index::PostingIterator *iter, 
                              const common::Term &term,
                              fieldmap_t fieldMap,
                              FieldMatchOperatorType opteratorType);
    ~FieldMapTermQueryExecutor();
private:
    FieldMapTermQueryExecutor(const FieldMapTermQueryExecutor &);
    FieldMapTermQueryExecutor& operator=(const FieldMapTermQueryExecutor &);
public:
    std::string toString() const override;
private:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t& result) override;
private:
    fieldmap_t _fieldMap;
    FieldMatchOperatorType _opteratorType;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FieldMapTermQueryExecutor> FieldMapTermQueryExecutorPtr;

} // namespace search
} // namespace isearch

