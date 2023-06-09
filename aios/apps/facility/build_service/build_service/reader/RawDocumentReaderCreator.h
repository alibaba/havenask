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
#ifndef ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H
#define ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/ReaderManager.h"
#include "build_service/util/Log.h"
#include "indexlib/document/document_factory.h"

namespace build_service { namespace util {
class SwiftClientCreator;
typedef std::shared_ptr<SwiftClientCreator> SwiftClientCreatorPtr;
}} // namespace build_service::util

namespace build_service { namespace reader {

class HologresInterface;

class RawDocumentReaderCreator : public proto::ErrorCollector
{
public:
    RawDocumentReaderCreator(const util::SwiftClientCreatorPtr& swiftClientCreator,
                             std::shared_ptr<HologresInterface> hologresInterface = nullptr);
    virtual ~RawDocumentReaderCreator();

private:
    RawDocumentReaderCreator(const RawDocumentReaderCreator&) = delete;
    RawDocumentReaderCreator(RawDocumentReaderCreator&&) = delete;

public:
    // virtual for mock
    virtual RawDocumentReader* create(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                      const proto::PartitionId& partitionId,
                                      indexlib::util::MetricProviderPtr metricsProvider,
                                      const indexlib::util::CounterMapPtr& counterMap,
                                      const indexlib::config::IndexPartitionSchemaPtr schema = nullptr);

    // virtual for mock
    virtual RawDocumentReader* create(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                      const proto::PartitionId& partitionId,
                                      indexlib::util::MetricProviderPtr metricsProvider,
                                      const indexlib::util::CounterMapPtr& counterMap,
                                      const std::shared_ptr<indexlibv2::config::TabletSchema>& schema);

private:
    RawDocumentReader* createSingleSourceReader(const config::ResourceReaderPtr& resourceReader,
                                                const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
                                                indexlib::util::MetricProviderPtr metricProvider,
                                                const indexlib::util::CounterMapPtr& counterMap,
                                                const indexlib::config::IndexPartitionSchemaPtr schema,
                                                const std::shared_ptr<indexlibv2::config::TabletSchema>& schemaV2);

private:
    ReaderManagerPtr _readerManagerPtr;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::shared_ptr<HologresInterface> _hologresInterface = nullptr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocumentReaderCreator);

}} // namespace build_service::reader

#endif // ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H
