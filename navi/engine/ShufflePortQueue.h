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
#ifndef NAVI_SHUFFLEPORTQUEUE_H
#define NAVI_SHUFFLEPORTQUEUE_H

#include "navi/engine/PortDataQueue.h"

namespace navi {

class ShufflePortQueue : public PortDataQueues
{
public:
    ShufflePortQueue();
    ~ShufflePortQueue();
private:
    ShufflePortQueue(const ShufflePortQueue &);
    ShufflePortQueue &operator=(const ShufflePortQueue &);
public:
    ErrorCode setEof(NaviPartId fromPartId) override;
    ErrorCode push(NaviPartId toPartId, NaviPartId fromPartId,
                   const DataPtr &data, bool eof, bool &retEof) override;
    ErrorCode pop(NaviPartId toPartId, DataPtr &data, bool &eof) override;
};
}

#endif //NAVI_SHUFFLEPORTQUEUE_H
