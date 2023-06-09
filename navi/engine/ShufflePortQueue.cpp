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
#include "navi/engine/ShufflePortQueue.h"
#include "navi/engine/Port.h"

namespace navi {

ShufflePortQueue::ShufflePortQueue() {
    _logger.addPrefix("shuffle");
}

ShufflePortQueue::~ShufflePortQueue() {
}

ErrorCode ShufflePortQueue::setEof(NaviPartId fromPartId) {
    auto retEc = EC_NONE;
    for (auto toPartId : _partInfo) {
        auto ec = doSetEof(toPartId, fromPartId);
        if (EC_NONE != ec && EC_NONE == retEc) {
            retEc = ec;
        }
    }
    return retEc;
}

ErrorCode ShufflePortQueue::push(NaviPartId toPartId, NaviPartId fromPartId,
                                 const DataPtr &data, bool eof, bool &retEof)
{
    assert(false);
    return EC_UNKNOWN;
}

ErrorCode ShufflePortQueue::pop(NaviPartId toPartId, DataPtr &data, bool &eof) {
    assert(false);
    return EC_UNKNOWN;
}

}
