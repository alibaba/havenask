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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

class ITabletImporter
{
public:
    virtual ~ITabletImporter() = default;

    virtual Status Check(const std::vector<Version>& versions, const Version* baseVersion, const ImportOptions& options,
                         std::vector<Version>* validVersions) = 0;
    virtual Status Import(const std::vector<Version>& versions, const Fence* fence, const ImportOptions& options,
                          Version* baseVersion) = 0;
};

} // namespace indexlibv2::framework
