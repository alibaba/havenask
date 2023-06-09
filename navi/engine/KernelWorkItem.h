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
#ifndef NAVI_KERNELWORKITEM_H
#define NAVI_KERNELWORKITEM_H

#include "navi/common.h"
#include "navi/engine/NaviWorkerBase.h"

namespace navi {

class TestEngine;
class Node;

class KernelWorkItem : public NaviWorkerItem
{
public:
    KernelWorkItem(NaviWorkerBase *worker,
                   Node *node);
public:
    void doProcess() override;
    friend class TestEngine;
private:
    Node *_node;
};

}

#endif //NAVI_KERNELWORKITEM_H
