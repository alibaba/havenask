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

#include "navi/builder/EImpl.h"

namespace navi {

class InterEImpl : public EImpl
{
public:
    InterEImpl();
    ~InterEImpl();
    InterEImpl(const InterEImpl &) = delete;
    InterEImpl &operator=(const InterEImpl &) = delete;
public:
    SingleE *addDownStream(P to) override;
    N split(const std::string &splitKernel) override;
    N merge(const std::string &mergeKernel) override;
private:
    void fillScopeBorder(P to) const;
private:
    static uint64_t getEdgeKey(P to) {
        return (((uint64_t)to._index) << 48) | ((uint64_t)to._impl);
    }
private:
    std::unordered_map<uint64_t, SingleE *> _edgeMap;
};

}

