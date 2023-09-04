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
#include "service/CarbonOpsServiceImpl.h"
#include "carbon/ErrorDefine.h"
#include "carbon/Ops.h"
#include "hippo/ProtoWrapper.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
USE_CARBON_NAMESPACE(master);

BEGIN_CARBON_NAMESPACE(service);

CARBON_LOG_SETUP(service, CarbonOpsServiceImpl);

CarbonOpsServiceImpl::CarbonOpsServiceImpl(
        const CarbonDriverPtr &carbonDriverPtr)
{
    _carbonDriverPtr = carbonDriverPtr;
}

CarbonOpsServiceImpl::~CarbonOpsServiceImpl() {
}


END_CARBON_NAMESPACE(service);

