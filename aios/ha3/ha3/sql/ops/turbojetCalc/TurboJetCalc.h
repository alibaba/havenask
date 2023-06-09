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

#include "ha3/sql/framework/PushDownOp.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/turbojetCalc/Common.h"
#include "table/Table.h"

namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

namespace isearch::sql {
class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
} // namespace isearch::sql

namespace turbojet {
class Program;
using ProgramPtr = std::shared_ptr<Program>;
class LogicalGraph;
using LogicalGraphPtr = std::shared_ptr<const LogicalGraph>;
namespace expr {
class SyntaxNode;
using SyntaxNodePtr = std::shared_ptr<const SyntaxNode>;
} // namespace expr
} // namespace turbojet

namespace isearch::sql::turbojet {

class TurboJetCalc;
using TurboJetCalcPtr = std::unique_ptr<TurboJetCalc>;

class TurboJetCalc : public PushDownOp {
public:
    TurboJetCalc(const CalcInitParam &param,
                 SqlBizResource *bizResource,
                 SqlQueryResource *queryResource,
                 MetaInfoResource *metaInfoResource,
                 navi::GraphMemoryPoolResource *memoryPoolResource);

public:
    void setReuseTable(bool reuse) override {
        _reuseTable = reuse;
    }
    bool isReuseTable() const override {
        return _reuseTable;
    }
    std::string getName() const override {
        return "turbojet_calc";
    }

    autil::Result<> doInit();
    bool init() override;

    autil::Result<> doCompute(suez::turing::FunctionProvider *functionProvider,
                              table::TablePtr &table,
                              bool &isEof);
    bool compute(table::TablePtr &table, bool &isEof) override;

private:
    CalcInitParam _param;
    SqlBizResource *_bizResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    MetaInfoResource *_metaInfoResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;

private:
    LogicalGraphPtr _node;
    std::map<std::string, expr::SyntaxNodePtr> _outMap;
    ProgramPtr _program;

private:
    bool _reuseTable {false};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace isearch::sql::turbojet
