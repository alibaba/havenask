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

class BorderEdgeDef;

class BorderEImpl : public EImpl
{
public:
    BorderEImpl();
    ~BorderEImpl();
    BorderEImpl(const BorderEImpl &) = delete;
    BorderEImpl &operator=(const BorderEImpl &) = delete;
public:
    SingleE *addDownStream(P to) override;
    N split(const std::string &splitKernel) override;
    N merge(const std::string &mergeKernel) override;
private:
    SingleE *addWithBorder(P to);
    SingleE *addWithoutBorder(P to);
    void addSplit();
    SingleE *addMerge(P to);
private:
    friend std::ostream &operator<<(std::ostream &os, const BorderEImpl &edge);
private:
    BorderEdgeDef *_fromBorder;
    BorderEdgeDef *_toBorder;
};

}

