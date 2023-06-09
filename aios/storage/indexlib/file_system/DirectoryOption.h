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

#include <stdint.h>

namespace indexlib { namespace file_system {

// legacy: recursive, packageHint
struct DirectoryOption {
    bool recursive = true; // ignore exist error if recursive = true
    bool packageHint = false;
    bool isMem = false;

    static DirectoryOption Local(bool recursive_, bool packageHint_)
    {
        DirectoryOption directoryOption;
        directoryOption.recursive = recursive_;
        directoryOption.packageHint = packageHint_;
        return directoryOption;
    }

    static DirectoryOption Mem()
    {
        DirectoryOption directoryOption;
        directoryOption.isMem = true;
        return directoryOption;
    }

    static DirectoryOption Package()
    {
        DirectoryOption directoryOption;
        directoryOption.packageHint = true;
        return directoryOption;
    }

    static DirectoryOption PackageMem()
    {
        DirectoryOption directoryOption;
        directoryOption.isMem = true;
        directoryOption.packageHint = true;
        return directoryOption;
    }
};
}} // namespace indexlib::file_system
