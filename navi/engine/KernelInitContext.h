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
#ifndef NAVI_KERNELINITCONTEXT_H
#define NAVI_KERNELINITCONTEXT_H

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/engine/AsyncPipe.h"
#include "autil/StringUtil.h"
#include <memory>

namespace navi {

class LocalSubGraph;
class Port;
class Node;

class KernelInitContext
{
private:
    KernelInitContext(Node *node);
public:
    TimeoutChecker *getTimeoutChecker() const;
    PoolPtr getPool() const;
    bool getInputNode(PortIndex index, std::string &node);
    bool getIORange(IoType ioType, const std::string &name, PortIndex &start,
                    PortIndex &end) const;
    bool getInputGroupSize(IndexType group, size_t &size);
    bool getOutputGroupSize(IndexType group, size_t &size);
    AsyncPipePtr createAsyncPipe(bool optional = false);
public:
    const ResourceMap &getDependResourceMap() const;
    template <typename T>
    T *getDependResource(const std::string &resourceName) const;
    const std::string &getConfigPath() const;
    const std::string &getThisBizName() const;
    NaviPartId getThisPartId() const;
private:
    friend class KernelComputeContext;
    friend class Node;
    friend class InputPortKernel;
    friend class OutputPortKernel;
    friend class TerminatorKernel;
    friend class PortSplitKernel;
    friend class PortMergeKernel;
    friend class ResourceCreateKernel;
    friend class ResourceSaveKernel;
    friend class NaviTesterInputKernel;
    friend class NaviTesterOutputKernel;
    friend class NaviTesterControlKernel;
    friend class KernelTester;
protected:
    Node *_node;
    LocalSubGraph *_subGraph;
    Port *_port;
    const ResourceMap &_resourceMap;
};

template <typename T>
T *KernelInitContext::getDependResource(const std::string &resourceName) const {
    return _resourceMap.getResource<T>(resourceName);
}

NAVI_TYPEDEF_PTR(KernelInitContext);

}

#endif //NAVI_KERNELINITCONTEXT_H
