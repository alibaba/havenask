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

#include "autil/Log.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib::file_system {

struct DeleteOption {
    FenceContext* fenceContext = FenceContext::NoFence(); // do fence check, if invalid, return error
    bool mayNonExist = false;                             // if true, if non exist, ignore and return ok

    static DeleteOption MayNonExist()
    {
        DeleteOption deleteOption;
        deleteOption.mayNonExist = true;
        return deleteOption;
    }

    static DeleteOption NoFence(bool mayNonExist)
    {
        DeleteOption deleteOption;
        deleteOption.mayNonExist = mayNonExist;
        return deleteOption;
    }

    static DeleteOption Fence(FenceContext* fenceContext, bool mayNonExist)
    {
        DeleteOption deleteOption;
        deleteOption.fenceContext = fenceContext;
        deleteOption.mayNonExist = mayNonExist;
        return deleteOption;
    }
};

} // namespace indexlib::file_system
