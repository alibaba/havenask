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
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::framework {

class DropIndexCleaner : private autil::NoCopyable
{
public:
    DropIndexCleaner();
    ~DropIndexCleaner();

public:
    static std::vector<std::string> CaclulateDropIndexDirs(const std::shared_ptr<TabletData>& originTabletData,
                                                           const std::shared_ptr<config::ITabletSchema>& targetSchema);
    static Status CleanIndexInLogical(const std::shared_ptr<TabletData>& originTabletData,
                                      const std::shared_ptr<config::ITabletSchema>& targetSchema,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& rootDirectory);
    static Status DropPrivateFence(const std::string& localRoot);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
