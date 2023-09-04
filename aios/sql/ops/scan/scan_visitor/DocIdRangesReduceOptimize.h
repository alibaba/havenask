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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "sql/ops/condition/ConditionVisitor.h"
#include "sql/ops/scan/KeyRange.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace sql {
class AndCondition;
class FieldInfo;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql

namespace suez {
class SortDescription;
} // namespace suez

namespace sql {

class DocIdRangesReduceOptimize : public ConditionVisitor {
public:
    DocIdRangesReduceOptimize(const std::vector<suez::SortDescription> &sortDescs,
                              const std::map<std::string, FieldInfo> &fieldInfos);
    ~DocIdRangesReduceOptimize();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
    isearch::search::LayerMetaPtr
    reduceDocIdRange(const isearch::search::LayerMetaPtr &lastRange,
                     autil::mem_pool::Pool *pool,
                     isearch::search::IndexPartitionReaderWrapperPtr &readerPtr);

private:
    void swapKey2KeyRange(std::unordered_map<std::string, KeyRangeBasePtr> &other) {
        std::swap(_key2keyRange, other);
    }
    void parseArithmeticOp(const autil::SimpleValue &param, const std::string &op);
    void tryAddKeyRange(const autil::SimpleValue &attr,
                        const autil::SimpleValue &value,
                        const std::string &op);
    template <typename T>
    static KeyRangeTyped<T> *genKeyRange(const std::string &attrName,
                                         const std::string &op,
                                         const autil::SimpleValue &value);
    indexlib::table::DimensionDescriptionVector convertDimens();
    std::string toDebugString(const indexlib::table::DimensionDescriptionVector &dimens);

private:
    std::vector<std::string> _keyVec;
    const std::map<std::string, FieldInfo> &_fieldInfos;
    std::unordered_map<std::string, KeyRangeBasePtr> _key2keyRange;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DocIdRangesReduceOptimize> DocIdRangesReduceOptimizePtr;
} // namespace sql
