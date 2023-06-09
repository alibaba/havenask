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
#include "indexlib/table/kkv_table/KKVTabletWriter.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVTabletOptions.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KKVTabletWriter);
#define TABLET_LOG(level, format, args...)                                                                             \
    assert(_tabletData);                                                                                               \
    AUTIL_LOG(level, "[%s] [%p]" format, _tabletData->GetTabletName().c_str(), this, ##args)

KKVTabletWriter::KKVTabletWriter(const std::shared_ptr<config::TabletSchema>& schema,
                                 const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
}

KKVTabletWriter::~KKVTabletWriter() {}

std::shared_ptr<KKVTabletReader> KKVTabletWriter::CreateKKVTabletReader()
{
    return std::make_shared<KKVTabletReader>(_schema);
}

Status KKVTabletWriter::OpenTabletReader()
{
    _tabletReader = CreateKKVTabletReader();
    ReadResource readResource;
    auto st = _tabletReader->Open(_tabletData, readResource);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "tablet reader open failed");
        return st;
    }
    auto indexConfigs = _schema->GetIndexConfigs();

    for (const auto& indexConfig : indexConfigs) {
        auto indexName = indexConfig->GetIndexName();
        if (KKV_RAW_KEY_INDEX_NAME == indexName) {
            continue;
        }
        auto indexReader = _tabletReader->GetIndexReader(indexConfig->GetIndexType(), indexName);
        auto kkvReader = std::dynamic_pointer_cast<KKVReader>(indexReader);
        auto kkvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
        if (!kkvReader || !kkvIndexConfig) {
            auto status = Status::InternalError("kkv reader dynamic cast failed, indexName: [%s]", indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        if (_schema->GetSchemaId() != kkvReader->GetReaderSchemaId()) {
            auto status = Status::InternalError("schema id not equal to kkv reader's schema id, indexName: [%s]",
                                                indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        auto indexNameHash = config::IndexConfigHash::Hash(kkvIndexConfig);
        _kkvReaders[indexNameHash] = kkvReader;
    }

    return Status::OK();
}

Status KKVTabletWriter::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                               const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions)
{
    if (_options->IsOnline()) {
        return OpenTabletReader();
    }
    // lazy open tablet reader when updateToAdd happened for offline.
    return Status::OK();
}

} // namespace indexlibv2::table
