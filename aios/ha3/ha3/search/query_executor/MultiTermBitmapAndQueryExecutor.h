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
#include "ha3/search/BitmapAndQueryExecutor.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace isearch {
namespace search {

class MultiTermBitmapAndQueryExecutor : public BitmapAndQueryExecutor {
public:
    MultiTermBitmapAndQueryExecutor(autil::mem_pool::Pool *pool)
        : BitmapAndQueryExecutor(pool) {}
    ~MultiTermBitmapAndQueryExecutor();

public:
    const std::string getName() const {
        return "MultiTermBitmapAndQueryExecutor";
    }
    std::string toString() const {
        return "MultiTerm" + BitmapAndQueryExecutor::toString();
    }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MultiTermBitmapAndQueryExecutor> MultiTermBitmapAndQueryExecutorPtr;

} // namespace search
} // namespace isearch
