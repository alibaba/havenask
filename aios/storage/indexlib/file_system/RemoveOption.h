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

#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib::file_system {

// legacy: mayNonExist, fenceContext
struct RemoveOption {
    FenceContext* fenceContext = FenceContext::NoFence(); // do fence check, if invalid, return error
    bool mayNonExist = false;                             // if true, if non exist, ignore and return ok
    bool relaxedConsistency = false; // if true, will use fine-grained fileSystem lock, the file not in entryTable may
                                     // still exsist for a while.
    bool logicalDelete = false;      // if true, only delete file in entry table, user will not see it

    static RemoveOption MayNonExist()
    {
        RemoveOption removeOption;
        removeOption.mayNonExist = true;
        return removeOption;
    }

    static RemoveOption Fence(FenceContext* fenceContext)
    {
        RemoveOption removeOption;
        removeOption.fenceContext = fenceContext;
        return removeOption;
    }
};

} // namespace indexlib::file_system
