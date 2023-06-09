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
#include "ha3/sql/data/SqlGraphType.h"
#include "ha3/sql/data/SqlGraphData.h"
#include "ha3/sql/common/Log.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/GraphDef.pb.h"

using namespace std;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, SqlGraphType);

const std::string SqlGraphType::TYPE_ID = "ha3.sql.sql_graph_type_id";

SqlGraphType::SqlGraphType()
    : Type(TYPE_ID)
{}

navi::TypeErrorCode SqlGraphType::serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const
{
    auto *sqlGraphData = dynamic_cast<SqlGraphData *>(data.get());
    if (sqlGraphData == nullptr || sqlGraphData->getGraphDef() == nullptr) {
        SQL_LOG(ERROR, "sql graph data is empty");
        return navi::TEC_NULL_DATA;
    }
    auto dataBuffer = ctx.getDataBuffer();
    dataBuffer.write(sqlGraphData->getOutputPorts());
    dataBuffer.write(sqlGraphData->getOutputNodes());
    string graphDefStr;
    sqlGraphData->getGraphDef()->SerializeToString(&graphDefStr);
    dataBuffer.write(graphDefStr);
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlGraphType::deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const {
    vector<string> outputPorts;
    vector<string> outputNodes;
    std::unique_ptr<navi::GraphDef> graphDef(new navi::GraphDef());
    auto dataBuffer = ctx.getDataBuffer();
    dataBuffer.read(outputPorts);
    dataBuffer.read(outputNodes);
    string graphDefStr;
    dataBuffer.read(graphDefStr);
    if (!graphDef->ParseFromString(graphDefStr)) {
        SQL_LOG(ERROR, "deserialize graphdef error, def [%s]", graphDefStr.c_str());
        return navi::TEC_NULL_DATA;
    }
    SqlGraphDataPtr sqlGraphData(new SqlGraphData(outputPorts, outputNodes, graphDef));
    data = sqlGraphData;
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlGraphType);

} //end namespace data
} //end namespace isearch
