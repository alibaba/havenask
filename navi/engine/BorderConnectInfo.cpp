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
#include "navi/engine/BorderConnectInfo.h"
#include "navi/util/CommonUtil.h"

namespace navi {

std::ostream& operator<<(std::ostream& os,
                         const BorderConnectInfo &connectInfo)
{
    return os << "BorderConnectInfo ("
              << CommonUtil::getPortQueueType(connectInfo.queueType) << ","
              << CommonUtil::getStoreType(connectInfo.storeType) << ","
              << "from[" << connectInfo.fromPartInfo.ShortDebugString() << "],"
              << "to[" << connectInfo.toPartInfo.ShortDebugString() << "],"
              << "eof[" << connectInfo.eofPartInfo.ShortDebugString() << "])";
}

}
