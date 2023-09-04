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
#include "suez_navi/resource/AsyncExecutorR.h"

namespace suez_navi {

const std::string AsyncExecutorR::RESOURCE_ID = "async_executor_r";

AsyncExecutorR::AsyncExecutorR()
    : navi::RootResource(RESOURCE_ID)
{
}

AsyncExecutorR::AsyncExecutorR(future_lite::Executor *asyncInterExecutor,
                               future_lite::Executor *asyncIntraExecutor)
    : navi::RootResource(RESOURCE_ID)
    , _asyncInterExecutor(asyncInterExecutor)
    , _asyncIntraExecutor(asyncIntraExecutor)
{
}

AsyncExecutorR::~AsyncExecutorR() {
}

REGISTER_RESOURCE(AsyncExecutorR);

}

