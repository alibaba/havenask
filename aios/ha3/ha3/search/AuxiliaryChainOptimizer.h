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
#include "ha3/common/Term.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "ha3/search/Optimizer.h"

namespace isearch {
namespace common {
class QueryClause;
}  // namespace common
namespace search {
class IndexPartitionReaderWrapper;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

class AuxiliaryChainOptimizer : public Optimizer
{
public:
    AuxiliaryChainOptimizer();
    ~AuxiliaryChainOptimizer();
public:
    bool init(OptimizeInitParam *param) override;
    std::string getName() const override {
        return "AuxChain";
    }
    OptimizerPtr clone() override;
    bool optimize(OptimizeParam *param) override;
    void disableTruncate() override;
private:
    void getTermDFs(const common::TermVector &termVector,
                    IndexPartitionReaderWrapper *readerWrapper,
                    TermDFMap &termDFMap);
    void insertNewQuery(const TermDFMap &termDFMap, common::QueryClause *queryClause);
public:
    // for test
    bool isActive() const { return _active; }
private:
    static QueryInsertType covertQueryInsertType(const std::string &str);
    static SelectAuxChainType covertSelectAuxChainType(const std::string &str);
private:
    QueryInsertType _queryInsertType;
    SelectAuxChainType _selectAuxChainType;
    std::string _auxChainName;
    bool _active;
private:
    friend class AuxiliaryChainOptimizerTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AuxiliaryChainOptimizer> AuxiliaryChainOptimizerPtr;

} // namespace search
} // namespace isearch

