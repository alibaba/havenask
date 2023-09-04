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
#include "indexlib/index/kv/KVSegmentReaderCreator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/AdapterKVSegmentReader.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.util, KVSegmentReaderCreator);

std::pair<Status, std::unique_ptr<IKVIterator>>
KVSegmentReaderCreator::CreateIterator(const std::shared_ptr<framework::Segment>& segment,
                                       const std::shared_ptr<config::KVIndexConfig>& indexConfig, schemaid_t schemaId,
                                       const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                                       bool disablePlainFormat)
{
    auto [s, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
    if (!s.IsOK()) {
        return {s, nullptr};
    }

    auto diskIndexer = std::dynamic_pointer_cast<KVDiskIndexer>(indexer);
    if (!diskIndexer) {
        return {Status::InternalError("not an disk indexer"), nullptr};
    }

    auto segmentSchema = segment->GetSegmentSchema();
    if (!segmentSchema) {
        return {Status::InternalError("segment_%d does not have schema", segment->GetSegmentId()), nullptr};
    }
    schemaid_t segSchemaId = segmentSchema->GetSchemaId();
    if (segSchemaId > schemaId) {
        auto s = Status::InternalError("segSchemaId:%d, merge schemaId: %d", (int)segSchemaId, (int)schemaId);
        return {std::move(s), nullptr};
    }
    std::vector<std::string> ignoreFields;
    if (ignoreFieldCalculator) {
        ignoreFields = ignoreFieldCalculator->GetIgnoreFields(segSchemaId, schemaId);
    }
    std::shared_ptr<IKVSegmentReader> segmentReader;
    std::tie(s, segmentReader) = CreateSegmentReader(diskIndexer->GetReader(), diskIndexer->GetIndexConfig(),
                                                     indexConfig, ignoreFields, disablePlainFormat);
    if (!s.IsOK()) {
        return {std::move(s), nullptr};
    }
    return {std::move(s), segmentReader->CreateIterator()};
}

pair<Status, shared_ptr<IKVSegmentReader>> KVSegmentReaderCreator::CreateSegmentReader(
    const shared_ptr<IKVSegmentReader>& segReader, const shared_ptr<KVIndexConfig>& segmentIndexConfig,
    const shared_ptr<KVIndexConfig>& readerIndexConfig, const vector<string>& ignoreFields, bool disablePlainFormat)
{
    auto currentConfig = segmentIndexConfig->GetValueConfig();
    auto targetConfig = readerIndexConfig->GetValueConfig();
    auto adapter = std::make_shared<PackValueAdapter>();
    auto [status, currentPackConfig] = currentConfig->CreatePackAttributeConfig();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "current config create pack attribute config fail");
        return std::make_pair(status, nullptr);
    }
    auto [status1, targetPackConfig] = targetConfig->CreatePackAttributeConfig();
    if (!status1.IsOK()) {
        AUTIL_LOG(ERROR, "target config create pack attribute config fail");
        return std::make_pair(status, nullptr);
    }
    if (!adapter->Init(currentPackConfig, targetPackConfig, ignoreFields)) {
        return std::make_pair(Status::InternalError("init PackValueAdapter fail"), std::shared_ptr<IKVSegmentReader>());
    }
    if (!adapter->NeedConvert()) {
        return std::make_pair(Status::OK(), segReader);
    }

    if (currentConfig->IsSimpleValue() || targetConfig->IsSimpleValue()) {
        AUTIL_LOG(ERROR, "not support simple value format");
        return std::make_pair(Status::InternalError("not support simple value format"), nullptr);
    }
    AUTIL_LOG(INFO, "Load AdapterSegmentReader with ignoreFields [%s]",
              autil::StringUtil::toString(ignoreFields, ",").c_str());
    if (disablePlainFormat) {
        adapter->DisablePlainFormat();
    }
    return std::make_pair(Status::OK(),
                          std::make_shared<AdapterKVSegmentReader>(segReader, adapter, currentConfig, targetConfig));
}

} // namespace indexlibv2::index
