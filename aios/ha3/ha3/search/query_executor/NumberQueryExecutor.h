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

#include <string>

#include "indexlib/misc/common.h"

#include "ha3/search/TermQueryExecutor.h"
#include "autil/Log.h" // IWYU pragma: keep

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

class NumberQueryExecutor : public TermQueryExecutor
{
public:
    NumberQueryExecutor(indexlib::index::PostingIterator *iter,
                        const common::Term &term);
    ~NumberQueryExecutor();
public:
    const std::string getName() const { return "NumberQueryExecutor";}
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch

