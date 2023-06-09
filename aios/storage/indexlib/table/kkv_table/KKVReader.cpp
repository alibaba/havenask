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
#include "indexlib/table/kkv_table/KKVReader.h"

#include "autil/Log.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"

using namespace std;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.table, KKVReader);

KKVReader::KKVReader(schemaid_t readerSchemaId) : _readerSchemaId(readerSchemaId) {}

KKVReader::~KKVReader() {}

Status KKVReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                       const framework::TabletData* tabletData) noexcept
{
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    assert(_indexConfig);

    auto status = _indexOptions.Init(_indexConfig, tabletData);
    if (!status.IsOK()) {
        return status;
    }
    return DoOpen(_indexConfig, tabletData);
}

} // namespace indexlibv2::table
