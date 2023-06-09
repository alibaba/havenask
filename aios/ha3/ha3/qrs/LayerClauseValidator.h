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
#include "ha3/common/ErrorDefine.h"

namespace isearch {
namespace common {
class QueryLayerClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeInfos;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace qrs {

class LayerClauseValidator
{
public:
    LayerClauseValidator(const suez::turing::AttributeInfos *attrInfos);
    ~LayerClauseValidator();
private:
    LayerClauseValidator(const LayerClauseValidator &);
    LayerClauseValidator& operator=(const LayerClauseValidator &);
public:
    bool validate(common::QueryLayerClause* queryLayerClause);
    ErrorCode getErrorCode() const;
    const std::string &getErrorMsg() const;
private:
    bool isRangeKeyWord(const std::string &keyName);
    bool checkRange(const common::QueryLayerClause *layerClause);
    bool checkField(const common::QueryLayerClause* layerClause);
    bool checkSequence(const common::QueryLayerClause* layerClause);
public:
    // public for test
    void autoAddSortedRange(common::QueryLayerClause* layerClause);
private:
    const suez::turing::AttributeInfos *_attrInfos;
    ErrorCode _errorCode;
    std::string _errorMsg;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerClauseValidator> LayerClauseValidatorPtr;

} // namespace qrs
} // namespace isearch

