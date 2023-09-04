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

#include "navi/common.h"
#include "navi/engine/Biz.h"
#include "navi/engine/GraphDomain.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Port.h"
#include "navi/engine/SubGraphBorder.h"
#include <atomic>

namespace navi {

class ReplaceInfoMap {
public:
    ReplaceInfoMap() {
    }
    void init(const SubGraphDef *subGraphDef);
    const std::string &getReplaceRTo(const std::string &from) const;
    const std::vector<std::string> *getReplaceRFrom(const std::string &to) const;
    const std::unordered_map<std::string, std::string> &getReplaceFrom2To() const {
        return _replaceFrom2To;
    }
    void replace(const std::map<std::string, bool> *dependMap, ResourceMap &resourceMap) const;
private:
    std::unordered_map<std::string, std::string> _replaceFrom2To;
    std::unordered_map<std::string, std::vector<std::string>> _replaceTo2From;
};

struct RewriteInfo {
    BizPtr biz;
    ResourceMapPtr resourceMap;
    MultiLayerResourceMap multiLayerResourceMap;
    ReplaceInfoMap replaceInfoMap;
};

NAVI_TYPEDEF_PTR(RewriteInfo);

class Graph;

class SubGraphBase
{
public:
    SubGraphBase(SubGraphDef *subGraphDef,
                 GraphParam *param,
                 const GraphDomainPtr &domain,
                 const SubGraphBorderMapPtr &borderMap,
                 Graph *graph);
    virtual ~SubGraphBase();
private:
    SubGraphBase(const SubGraphBase &);
    SubGraphBase &operator=(const SubGraphBase &);
public:
    virtual ErrorCode init() = 0;
    virtual ErrorCode run() = 0;
    // graph specialized terminate logic
    virtual void terminate(ErrorCode ec) = 0;
public:
    void terminateThis(ErrorCode ec);
    GraphId getGraphId() const;
    void setErrorCode(ErrorCode ec);
    ErrorCode getErrorCode() const;
    NaviResultPtr getResult();
    Graph *getGraph() const;
    GraphParam *getParam() const;
    NaviWorkerBase *getWorker() const;
    bool terminated() const;
    const SubGraphDef &getSubGraphDef() const;
    const std::string &getBizName() const;
    NaviPartId getPartCount() const;
    NaviPartId getPartId() const;
protected:
    BizPtr getBiz(const SubGraphDef *subGraphDef);
    ResourceMapPtr getResourceMap(const SubGraphDef *subGraphDef) const;
private:
    // do not call this directly, use notifyFinish instead
    void terminateAll(ErrorCode ec);
protected:
    DECLARE_LOGGER();
    SubGraphDef *_subGraphDef;
    GraphParam *_param;
    GraphDomainPtr _domain;
    SubGraphBorderMapPtr _borderMap;
    Graph *_graph;
    std::atomic<bool> _terminated;
};

}
