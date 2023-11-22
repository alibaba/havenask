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

#include "navi/proto/GraphVis.pb.h"
#include "navi/engine/GraphMetric.h"
#include "navi/engine/GraphResultBase.h"
#include "navi/engine/RpcGraphResult.h"
#include "navi/engine/StringHashTable.h"
#include "navi/log/TraceCollector.h"
#include "navi/engine/NaviResult.h"

namespace navi {

class NaviMessage;
class NaviHostInfo;

class SerializedResult {
public:
    SerializedResultDef resultDef;
};
NAVI_TYPEDEF_PTR(SerializedResult);

class GraphResult : public GraphResultBase
{
public:
    GraphResult();
    virtual ~GraphResult();
private:
    GraphResult(const GraphResult &) = delete;
    GraphResult &operator=(const GraphResult &) = delete;
public:
    void init(const NaviLoggerPtr &logger, const std::string &formatPattern,
              const NaviSymbolTablePtr &naviSymbolTable,
              const NaviHostInfo *hostInfo);
    void setGraphDef(GraphDef *graphDef);
    GraphMetric *getGraphMetric() {
        return &_graphMetric;
    }
    const NaviHostInfo *getHostInfo() const {
        return _hostInfo;
    }
    NaviErrorPtr makeError(const NaviLoggerPtr &logger, ErrorCode ec) const;
    NaviErrorPtr updateError(const NaviErrorPtr &error);
    void setError(const NaviLoggerPtr &logger, ErrorCode ec);
    NaviErrorPtr getError() const;
public:
    NaviResultPtr createNaviResult();
    std::shared_ptr<multi_call::GigStreamRpcInfoMap> getRpcInfoMap();
    std::shared_ptr<GraphVisDef> getVisProto();
private:
    void doToProto(bool partial, NaviMessage &naviMessage) override;
    void markThisLocation(SingleResultLocationDef *singleLocation) const override;
    void doCollectTrace(TraceCollector &collector,
                        SymbolTableDef &symbolTableDef) override;
    void doToVisProto(GraphVisNodeDef *visDef) override;
private:
    void collectLoggerTrace();
    bool toGraphResultDef(bool partial, GraphResultDef &graphResultDef);
    void fillHostInfo(GraphResultDef &graphResultDef);
private:
    NaviLoggerPtr _logger;
    std::string _formatPattern;
    GraphMetric _graphMetric;
    const NaviHostInfo *_hostInfo = nullptr;
    GraphDef *_graphDef = nullptr;
    StringHashTable _stringHashTable;
    mutable autil::ThreadMutex _resultMutex;
    NaviErrorPtr _error;
    std::shared_ptr<GraphVisDef> _visProto;
    std::shared_ptr<multi_call::GigStreamRpcInfoMap> _rpcInfoMap;
};

NAVI_TYPEDEF_PTR(GraphResult);

}
