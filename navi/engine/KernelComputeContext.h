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
#ifndef NAVI_KERNELCOMPUTECONTEXT_H
#define NAVI_KERNELCOMPUTECONTEXT_H

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelInitContext.h"
#include "navi/engine/KernelMetric.h"
#include "navi/engine/NaviResult.h"
#include "navi/engine/RunGraphParams.h"
#include <vector>

namespace navi {

class Node;
class KernelComputeContext;
struct InputSnapshot;
class GraphDef;

class GroupDatas {
public:
    GroupDatas();
private:
    GroupDatas(InputSnapshot *begin, InputSnapshot *end);
public:
    class Iterator {
    public:
        Iterator();
        Iterator(InputSnapshot *ptr);
        Iterator operator++();
        Iterator operator+(size_t offset) const;
        bool operator!=(const Iterator & rhs) const;
        const StreamData &operator*();
    private:
        InputSnapshot *_ptr;
    };
public:
    Iterator begin() const;
    Iterator end() const;
    size_t size() const;
    const StreamData &operator[](size_t index);
    bool eof() const;
private:
    InputSnapshot *_begin;
    InputSnapshot *_end;
private:
    friend class KernelComputeContext;
};

class KernelComputeContext : public KernelInitContext
{
private:
    KernelComputeContext(Node *node);
private:
    KernelComputeContext(const KernelComputeContext &);
    KernelComputeContext& operator=(const KernelComputeContext &);
public:
    // input
    bool getInput(PortIndex index, DataPtr &data, bool &eof);
    bool getInput(PortIndex index, StreamData &streamData);
    bool getGroupInput(IndexType group, GroupDatas &datas);
    // output
    bool setOutput(PortIndex index, const DataPtr &data, bool eof);
    // todo
    // bool collectOutput(PortIndex index, const DataPtr &data, bool eof);
    // misc
    bool deadLock() const;
    void setIgnoreDeadlock();
    ErrorCode fork(GraphDef *graphDef, const std::vector<OverrideData> &overrideData = {});
    const KernelMetric &getMetric() const;
    void appendResult(NaviResult &result);
private:
    friend class Node;
    friend class KernelTester;
private:
    KernelMetric *_metric;
    bool _deadLock;
    bool _ignoreDeadlock;
};

NAVI_TYPEDEF_PTR(KernelComputeContext);

}

#endif //NAVI_KERNELCOMPUTECONTEXT_H
