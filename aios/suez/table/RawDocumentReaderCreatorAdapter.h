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

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "suez/table/wal/WALConfig.h"

namespace suez {

class RawDocumentReaderCreatorAdapter {
public:
    RawDocumentReaderCreatorAdapter(const build_service::util::SwiftClientCreatorPtr &swiftClientCreator,
                                    const WALConfig &walConfig);
    ~RawDocumentReaderCreatorAdapter();

private:
    RawDocumentReaderCreatorAdapter(const RawDocumentReaderCreatorAdapter &);
    RawDocumentReaderCreatorAdapter &operator=(const RawDocumentReaderCreatorAdapter &);

public:
    build_service::reader::RawDocumentReader *
    create(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader,
           const build_service::KeyValueMap &kvMap,
           const build_service::proto::PartitionId &partitionId,
           indexlib::util::MetricProviderPtr metricsProvider,
           const indexlib::util::CounterMapPtr &counterMap,
           const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);

private:
    build_service::reader::RawDocumentReader *
    createQueueRawDoucmentReader(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader,
                                 const build_service::KeyValueMap &kvMap,
                                 const build_service::proto::PartitionId &partitionId,
                                 indexlib::util::MetricProviderPtr metricsProvider,
                                 const indexlib::util::CounterMapPtr &counterMap,
                                 const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);

private:
    std::unique_ptr<build_service::reader::RawDocumentReaderCreator> _readerCreator;
    WALConfig _walConfig;
};

} // namespace suez
