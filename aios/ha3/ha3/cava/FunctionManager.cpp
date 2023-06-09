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
#include "ha3/cava/FunctionManager.h"
#include "autil/mem_pool/Pool.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

using namespace std;
using namespace suez::turing;

namespace ha3 {

AUTIL_LOG_SETUP(ha3, FunctionManager);

FunctionManager::FunctionManager(
        const FunctionInterfaceCreator *funcCreator,
        FunctionProvider *provider,
        autil::mem_pool::Pool *pool,
        const suez::turing::CavaPluginManager *cavaPluginManager)
    : suez::turing::FunctionManager(funcCreator, provider, pool, cavaPluginManager)
{
}

FunctionManager::~FunctionManager() {
}
}
