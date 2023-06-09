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
#include "indexlib/index/attribute/merger/SingleValueAttributeMerger.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyAttributeMerger : public SingleValueAttributeMerger<Key>
{
public:
    PrimaryKeyAttributeMerger(const std::shared_ptr<config::IIndexConfig>& pkIndexConfig)
        : _pkIndexConfig(pkIndexConfig)
    {
    }

    ~PrimaryKeyAttributeMerger() = default;

private:
    std::pair<Status, std::shared_ptr<IIndexer>>
    GetDiskIndexer(const std::shared_ptr<framework::Segment>& segment) override;

    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir) override;

private:
    std::shared_ptr<config::IIndexConfig> _pkIndexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
