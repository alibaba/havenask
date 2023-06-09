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
namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system
namespace indexlibv2 { namespace framework {
class TabletData;
}} // namespace indexlibv2::framework
namespace indexlibv2 { namespace index {
class DeletionMapDiskIndexer;
}} // namespace indexlibv2::index
namespace indexlibv2::table {

class PatchedDeletionMapLoader
{
public:
    static Status GetPatchedDeletionMapDiskIndexers(
        const std::shared_ptr<framework::TabletData>& tabletData,
        const std::shared_ptr<indexlib::file_system::Directory>& patchDir,
        std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>>* indexPairs);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
