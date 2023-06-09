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

#include "ha3/sql/data/SqlGraphType.h"
#include "navi/engine/Data.h"
#include "navi/proto/GraphDef.pb.h"

namespace isearch {
namespace sql {

class SqlGraphData : public navi::Data
{
public:
    SqlGraphData(const std::vector<std::string> &portVec,
                 const std::vector<std::string> &nodeVec,
                 std::unique_ptr<navi::GraphDef>& graphDef)
        : navi::Data(SqlGraphType::TYPE_ID, nullptr)
        , _outputPorts(portVec)
        , _outputNodes(nodeVec)
        , _graphDef(std::move(graphDef))
    {}
    ~SqlGraphData() {}
private:
    SqlGraphData(const SqlGraphData &) = delete;
    SqlGraphData& operator=(const SqlGraphData &) = delete;
public:
    std::unique_ptr<navi::GraphDef> &getGraphDef() {
        return _graphDef;
    }
    const std::vector<std::string> &getOutputPorts() {
        return _outputPorts;
    }
    const std::vector<std::string> &getOutputNodes() {
        return _outputNodes;
    }
private:
    std::vector<std::string> _outputPorts;
    std::vector<std::string> _outputNodes;
    std::unique_ptr<navi::GraphDef> _graphDef;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlGraphData> SqlGraphDataPtr;

} //end namespace data
} //end namespace isearch
