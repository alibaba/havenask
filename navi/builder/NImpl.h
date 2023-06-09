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

#include <unordered_set>
#include "autil/legacy/json.h"
#include "navi/builder/NImplBase.h"
#include "navi/common.h"

namespace navi {

class NImpl : public NImplBase
{
public:
    NImpl();
    ~NImpl();
    NImpl(const NImpl &) = delete;
    NImpl &operator=(const NImpl &) = delete;
public:
    void init(GraphBuilder *builder, GraphId graphId, NodeDef *def, NodeType nodeType);
    void kernel(const std::string &name) override;
    void device(const std::string &device) override;
    void jsonAttrs(const std::string &attrs) override;
    void jsonAttrsFromMap(const std::map<std::string, std::string> &attrs) override;
    void jsonAttrsFromJsonMap(const autil::legacy::json::JsonMap &attrs) override;
    void binaryAttrsFromMap(const std::map<std::string, std::string> &binaryAttrs) override;
    void attr(const std::string &key, const std::string &value) override;
    void integerAttr(const std::string &key, int64_t value) override;
    void skipConfig(bool skipConfig) override;
    void skipInit(bool skipInit) override;
    void stopAfterInit(bool stopAfterInit) override;
    void skipDeleteKernel(bool skipDeleteKernel) override;
public:
    P in(const std::string &port) override;
    P out(const std::string &port) override;
    void dependOn(P p) override;
public:
    GraphBuilder *builder() const override;
    const std::string &name() const override;
    const std::string &kernel() const override;
    GraphId graphId() const override;
private:
    void setJsonAttr(const std::string &attrStr);
private:
    friend std::ostream &operator<<(std::ostream &os, const NImpl &impl);
private:
    GraphBuilder *_builder;
    GraphId _graphId;
    NodeDef *_def;
    NodeType _nodeType;
    std::unordered_map<std::string, PImpl *> _ins;
    std::unordered_map<std::string, PImpl *> _outs;
    std::unordered_set<PImpl *> _dependOns;
};

}

