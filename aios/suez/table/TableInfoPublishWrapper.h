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

#include <map>

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"

namespace multi_call {
class GigRpcServer;
};

namespace suez {

class TableInfoPublishWrapper {
public:
    TableInfoPublishWrapper(multi_call::GigRpcServer *gigRpcServer);
    ~TableInfoPublishWrapper();

public:
    bool publish(const multi_call::ServerBizTopoInfo &info, multi_call::SignatureTy &signature);
    bool unpublish(multi_call::SignatureTy signature);
    bool updateVolatileInfo(multi_call::SignatureTy signature, const multi_call::BizVolatileInfo &info);

private:
    mutable autil::ThreadMutex _lock;
    multi_call::GigRpcServer *_gigRpcServer;
    std::map<multi_call::SignatureTy, int32_t> _counterMap;
};

} // namespace suez
