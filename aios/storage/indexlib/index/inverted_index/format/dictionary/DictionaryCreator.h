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

#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::framework {
class SegmentStatistics;
}

namespace indexlib::index {

class DictionaryReader;
class DictionaryWriter;

class DictionaryCreator
{
public:
    DictionaryCreator() = default;
    ~DictionaryCreator() = default;

    static DictionaryReader* CreateReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config);
    static DictionaryWriter*
    CreateWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                 const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStatistics,
                 autil::mem_pool::PoolBase* pool);
    // legacy for indexlib v1
    static DictionaryWriter* CreateWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                          const std::string& temperatureLayer, autil::mem_pool::PoolBase* pool);

    static DictionaryReader* CreateDiskReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
