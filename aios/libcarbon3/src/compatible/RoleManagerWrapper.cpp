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
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "compatible/RoleManagerWrapperImpl.h"

namespace carbon {

RoleManagerWrapper::RoleManagerWrapper() {}
RoleManagerWrapper::~RoleManagerWrapper() {}

RoleManagerWrapper* RoleManagerWrapper::getInstance() {
    return new compatible::RoleManagerWrapperImpl();
}

void RoleManagerWrapper::destroyInstance(
        RoleManagerWrapper *roleManagerWrapper)
{
    delete roleManagerWrapper;
}

}
