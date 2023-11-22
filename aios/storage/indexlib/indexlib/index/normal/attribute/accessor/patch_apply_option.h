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

#include <memory>

#include "indexlib/common_define.h"
DECLARE_REFERENCE_CLASS(index, AttributePatchReader);

namespace indexlib { namespace index {

/* PatchOnRead:
apply on read:
false: thread safe, patch will be apply on open(which requires mmap)
 */

enum PatchApplyStrategy : unsigned int {
    PAS_APPLY_UNKNOW,
    PAS_APPLY_NO_PATCH, // read function is thread safe, patch will not be applyed, for online, patch will be applied by
                        // patch loader
    PAS_APPLY_ON_READ,  // read function is not thread safe, patch will be readed on read, usually used for offline
                        // reader
};
struct PatchApplyOption {
    PatchApplyStrategy applyStrategy;
    AttributePatchReaderPtr patchReader;
    static PatchApplyOption NoPatch()
    {
        PatchApplyOption option;
        option.applyStrategy = PatchApplyStrategy::PAS_APPLY_NO_PATCH;
        return option;
    }
    static PatchApplyOption OnRead(AttributePatchReaderPtr patchReader)
    {
        PatchApplyOption option;
        option.applyStrategy = PatchApplyStrategy::PAS_APPLY_ON_READ;
        option.patchReader = patchReader;
        return option;
    }
};

}} // namespace indexlib::index
