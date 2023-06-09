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
#ifndef NAVI_MAPKERNEL_H
#define NAVI_MAPKERNEL_H

#include "navi/engine/Kernel.h"
#include "navi/engine/PipeKernel.h"

namespace navi {

class MapKernel : public PipeKernel
{
public:
    MapKernel(const std::string &name);
    ~MapKernel();
private:
    MapKernel(const MapKernel &);
    MapKernel &operator=(const MapKernel &);
public:
    ErrorCode compute(KernelComputeContext &ctx) override;
    virtual DataPtr map(const DataPtr &data) = 0;
    virtual void eof();
private:
    std::string _name;
};

}

#endif //NAVI_MAPKERNEL_H
