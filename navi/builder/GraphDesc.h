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
#include "autil/legacy/json.h"

namespace navi {

class NImplBase;
class P;

class N
{
private:
    N(NImplBase *impl);
public:
    ~N();
public:
    N &kernel(const std::string &name);
    N &device(const std::string &device);
    N &jsonAttrs(const std::string &attrs);
    N &jsonAttrsFromMap(const std::map<std::string, std::string> &attrs);
    N &jsonAttrsFromJsonMap(const autil::legacy::json::JsonMap &attrs);
    N &binaryAttrsFromMap(const std::map<std::string, std::string> &binaryAttrs);
    N &attr(const std::string &key, const std::string &value);
    N &integerAttr(const std::string &key, int64_t value);
    N &skipConfig(bool skipConfig);
    N &skipInit(bool skipInit);
    N &stopAfterInit(bool stopAfterInit);
    N &skipDeleteKernel(bool skipDeleteKernel);
public:
    P in(const std::string &port) const;
    P out(const std::string &port) const;
    void dependOn(P p);
public:
    const std::string &name() const;
    const std::string &kernel() const;
    GraphId graphId() const;
private:
    friend class SubGraphBuildInfo;
    friend class BorderEImpl;
    friend class InterEImpl;
private:
    NImplBase *_impl;
};

class PImpl;
class E;

class P
{
private:
    P(PImpl *impl, IndexType index);
    P();
public:
    ~P();
public:
    E from(P peer);
    E to(P peer);
    E fromForkNodeInput(const std::string &input, IndexType index = INVALID_INDEX);
    E toForkNodeOutput(const std::string &output, IndexType index = INVALID_INDEX);
    E asGraphOutput(const std::string &name);
public:
    P asGroup();
    P next();
    P index(IndexType index);
    P autoNext();
public:
    bool operator==(const P &other) const;
private:
    friend class NImpl;
    friend class NImplFake;
    friend class NImplFork;
    friend class PImpl;
    friend class BorderEImpl;
    friend class InterEImpl;
    friend class EImpl;
    friend class SubGraphBuildInfo;
public:
    friend std::ostream &operator<<(std::ostream &os, const P &p);
private:
    PImpl *_impl;
    IndexType _index;
};

class EImpl;
class SingleE;

class E
{
private:
    E(EImpl *e, SingleE *singleE);
public:
    ~E();
public:
    N split(const std::string &splitKernel);
    N merge(const std::string &mergeKernel);
    E require(bool require);
private:
    friend class PImpl;
    friend class BorderEImpl;
private:
    EImpl *_e;
    SingleE *_singleE;
};

}

