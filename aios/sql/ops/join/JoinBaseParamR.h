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

#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/ops/join/JoinInfoCollectorR.h"
#include "suez/turing/navi/QueryMemPoolR.h"

namespace sql {

class CalcTableR;
class JoinBase;

class JoinBaseParamR : public navi::Resource {
public:
    JoinBaseParamR();
    ~JoinBaseParamR();
    JoinBaseParamR(const JoinBaseParamR &) = delete;
    JoinBaseParamR &operator=(const JoinBaseParamR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void joinRow(size_t leftIndex, size_t rightIndex) {
        _tableAIndexes.emplace_back(leftIndex);
        _tableBIndexes.emplace_back(rightIndex);
    }
    void reserveJoinRow(size_t rowCount);

private:
    void patchHintInfo();
    bool convertFields();
    bool initCalcTableAndJoinColumns(navi::ResourceInitContext &ctx);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

public:
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(JoinInfoCollectorR, _joinInfoR);
    std::string _joinType;
    std::string _semiJoinType;
    std::string _conditionJson;
    std::string _equiConditionJson;
    std::string _remainConditionJson;
    std::vector<std::string> _leftInputFields;
    std::vector<std::string> _rightInputFields;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsForKernel;
    std::vector<std::string> _leftJoinColumns;
    std::vector<std::string> _rightJoinColumns;
    std::vector<size_t> _tableAIndexes;
    std::vector<size_t> _tableBIndexes;
    std::unordered_map<std::string, std::string> _defaultValue;
    std::map<std::string, std::pair<std::string, bool>> _output2InputMap;
    std::map<std::string, std::string> _joinHintMap;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    autil::mem_pool::Pool *_pool = nullptr;
    size_t _systemFieldNum = 0;
    std::shared_ptr<CalcTableR> _calcTableR;
    std::unique_ptr<JoinBase> _joinPtr;
};

} // namespace sql
