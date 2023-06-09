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

#include "navi/builder/NImplBase.h"

namespace navi {

class NImplFake : public NImplBase
{
public:
    NImplFake();
    ~NImplFake();
    NImplFake(const NImplFake &) = delete;
    NImplFake &operator=(const NImplFake &) = delete;
public:
    void init(GraphBuilder *builder,
              GraphId graphId,
              const std::string &name,
              const std::string &kernel,
              NodeType nodeType);
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
    friend std::ostream &operator<<(std::ostream &os, const NImplFake &impl);
private:
    GraphBuilder *_builder;
    GraphId _graphId;
    std::string _name;
    std::string _kernel;
    NodeType _nodeType;
};

}

