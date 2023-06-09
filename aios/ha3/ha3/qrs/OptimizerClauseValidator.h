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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"

namespace isearch {
namespace common {
class OptimizerClause;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace qrs {

class OptimizerClauseValidator
{
public:
    OptimizerClauseValidator();
    ~OptimizerClauseValidator();
public:

    ErrorCode getErrorCode();
    bool validate(const common::OptimizerClause* optimizerClause);

private:
    //bool validateRetHitsLimit(const common::OptimizerClause* optimizerClause);
    //bool validateResultFormat(const common::OptimizerClause* optimizerClause);

private:
    AUTIL_LOG_DECLARE();
    ErrorCode _errorCode;
    //uint32_t _retHitsLimit;

    friend class OptimizerClauseValidatorTest;
};


typedef std::shared_ptr<OptimizerClauseValidator> OptimizerClauseValidatorPtr;

} // namespace qrs
} // namespace isearch

