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
#ifndef NAVI_BORDERCONNECTINFO_H
#define NAVI_BORDERCONNECTINFO_H

#include "navi/common.h"
#include "navi/engine/PartInfo.h"

namespace navi {

struct BorderConnectInfo {
    BorderConnectInfo()
        : queueType(PQT_UNKNOWN)
        , storeType(PST_UNKNOWN)
    {
    }
    PortQueueType queueType;
    PortStoreType storeType;
    PartInfo fromPartInfo;
    PartInfo toPartInfo;
    PartInfo eofPartInfo;
};

extern std::ostream& operator<<(std::ostream& os,
                                const BorderConnectInfo &connectInfo);

}

#endif //NAVI_BORDERCONNECTINFO_H
