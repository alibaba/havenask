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
#include <utility>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMeta.h"

namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system

namespace indexlibv2::framework {
class TabletData;

class VersionMetaCreator : private autil::NoCopyable
{
public:
    static std::pair<Status, VersionMeta> Create(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                                 const TabletData& tabletData, const Version& version);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
