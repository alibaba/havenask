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
#include <string>
#include <utility>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::framework {
class IdGenerator;
class ITabletFactory;

class MemSegmentCreator
{
public:
    MemSegmentCreator(const std::string& tabletName, config::TabletOptions* tabletOptions,
                      ITabletFactory* tabletFactory, IdGenerator* idGenerator,
                      const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir);
    ~MemSegmentCreator();
    std::pair<Status, std::shared_ptr<MemSegment>>
    CreateMemSegment(const BuildResource& buildResource, const std::shared_ptr<TabletData>& tabletData,
                     const std::shared_ptr<config::ITabletSchema>& writeSchema);

private:
    Status CreateSegmentMeta(segmentid_t newSegId, const std::shared_ptr<TabletData>& tabletData,
                             const std::shared_ptr<config::ITabletSchema>& writeSchema, SegmentMeta* segMeta);
    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>> PrepareSegmentDir(const std::string& segDir);

private:
    std::string _tabletName;
    config::TabletOptions* _tabletOptions;
    ITabletFactory* _tabletFactory;
    IdGenerator* _idGenerator;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
