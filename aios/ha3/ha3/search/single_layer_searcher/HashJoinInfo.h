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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"

namespace matchdoc {
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class HashJoinInfo
{
public:
    typedef std::unordered_map<size_t, std::vector<int32_t>> HashJoinMap;
    using DocIdWrapper = suez::turing::DocIdWrapper;
    HashJoinInfo(const std::string &auxTableName,
                 const std::string &joinFieldName)
        : _auxTableName(auxTableName)
        , _joinFieldName(joinFieldName)
        , _joinAttrExpr(nullptr)
        , _auxDocidRef(nullptr) {}

    ~HashJoinInfo() {}

private:
    HashJoinInfo(const HashJoinInfo &);
    HashJoinInfo& operator=(const HashJoinInfo &);
public:
    void setAuxTableName(const std::string &auxTableName) {
	_auxTableName = auxTableName;
    }
    const std::string &getAuxTableName() const {
	return _auxTableName;
    }
    void setJoinFieldName(const std::string &joinFieldName) {
        _joinFieldName = joinFieldName;
    }
    const std::string &getJoinFieldName() const {
	return _joinFieldName;
    }
    void setJoinAttrExpr(suez::turing::AttributeExpression *joinAttrExpr) {
        _joinAttrExpr = joinAttrExpr;
    }
    suez::turing::AttributeExpression *getJoinAttrExpr() {
	return _joinAttrExpr;
    }
    void setAuxDocidRef(matchdoc::Reference<DocIdWrapper> *ref) {
	_auxDocidRef = ref;
    }
    matchdoc::Reference<DocIdWrapper> *getAuxDocidRef() const {
        return _auxDocidRef;
    }
    const HashJoinMap &getHashJoinMap() const { return _hashJoinMap; }
    HashJoinMap &getHashJoinMap() { return _hashJoinMap; }

private:
    std::string _auxTableName;
    std::string _joinFieldName;
    suez::turing::AttributeExpression *_joinAttrExpr;
    matchdoc::Reference<DocIdWrapper> *_auxDocidRef;
    HashJoinMap _hashJoinMap;
private:
    AUTIL_LOG_DECLARE();
};


typedef std::shared_ptr<HashJoinInfo> HashJoinInfoPtr;

} // namespace search
} // namespace isearch
