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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {

class VersionValidator
{
public:
    static Status Validate(const std::string& indexRootPath, const std::shared_ptr<const config::ITabletSchema>& schema,
                           versionid_t versionId);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
