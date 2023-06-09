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
#include "suez/table/RawDocumentReaderCreatorAdapter.h"

#include <autil/Log.h>

#include "build_service/reader/DocumentSeparators.h"
#include "suez/table/QueueRawDocumentReader.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RawDocumentReaderCreatorAdapter);

namespace suez {

RawDocumentReaderCreatorAdapter::RawDocumentReaderCreatorAdapter(
    const build_service::util::SwiftClientCreatorPtr &swiftClientCreator, const WALConfig &walConfig)
    : _readerCreator(std::make_unique<build_service::reader::RawDocumentReaderCreator>(swiftClientCreator))
    , _walConfig(walConfig) {}

RawDocumentReaderCreatorAdapter::~RawDocumentReaderCreatorAdapter() {}

build_service::reader::RawDocumentReader *
RawDocumentReaderCreatorAdapter::create(const shared_ptr<build_service::config::ResourceReader> &resourceReader,
                                        const build_service::KeyValueMap &kvMap,
                                        const build_service::proto::PartitionId &partitionId,
                                        indexlib::util::MetricProviderPtr metricsProvider,
                                        const indexlib::util::CounterMapPtr &counterMap,
                                        const std::shared_ptr<indexlibv2::config::TabletSchema> &schema) {
    const string &strategyName = _walConfig.strategy;
    AUTIL_LOG(INFO, "create raw document reader, wal strategy:%s", strategyName.c_str());
    if (strategyName != "queue") {
        return _readerCreator->create(resourceReader, kvMap, partitionId, metricsProvider, counterMap, schema);
    }

    return createQueueRawDoucmentReader(resourceReader, kvMap, partitionId, metricsProvider, counterMap, schema);
}

build_service::reader::RawDocumentReader *RawDocumentReaderCreatorAdapter::createQueueRawDoucmentReader(
    const shared_ptr<build_service::config::ResourceReader> &resourceReader,
    const build_service::KeyValueMap &kvMap,
    const build_service::proto::PartitionId &partitionId,
    indexlib::util::MetricProviderPtr metricsProvider,
    const indexlib::util::CounterMapPtr &counterMap,
    const std::shared_ptr<indexlibv2::config::TabletSchema> &schema) {
    build_service::reader::ReaderInitParam param;
    param.resourceReader = resourceReader;
    param.kvMap = kvMap;
    param.kvMap.insert(_walConfig.sinkDescription.begin(), _walConfig.sinkDescription.end());
    param.kvMap[build_service::reader::RAW_DOCUMENT_FORMAT] =
        build_service::reader::RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER;
    param.range = partitionId.range();
    param.metricProvider = metricsProvider;
    param.counterMap = counterMap;
    param.schema = nullptr;
    param.schemaV2 = schema;
    param.partitionId = partitionId;
    auto reader = new QueueRawDocumentReader();
    if (!reader->initialize(param)) {
        delete reader;
        return nullptr;
    }
    return reader;
}

} // namespace suez
