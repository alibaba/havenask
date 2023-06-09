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

#include "indexlib/table/kv_table/TabletDocIteratorBase.h"

namespace indexlibv2::config {
class KVIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::table {

class KVTabletDocIterator final : public TabletDocIteratorBase
{
public:
    KVTabletDocIterator() = default;
    ~KVTabletDocIterator() = default;

protected:
    Status InitFromTabletData(const std::shared_ptr<framework::TabletData>& tabletData) override;
    autil::StringView PreprocessPackValue(const autil::StringView& value) override;
    std::unique_ptr<index::IShardRecordIterator> CreateShardDocIterator() const override;

private:
    std::shared_ptr<config::KVIndexConfig> _kvIndexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
