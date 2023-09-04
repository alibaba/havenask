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
#include "build_service/reader/RawDocumentReaderCreator.h"

#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "beeper/beeper.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/BinaryFileRawDocumentReader.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/reader/FileRawDocumentReader.h"
#include "build_service/reader/IndexDocReader.h"
#include "build_service/reader/IndexDocToFileReader.h"
#include "build_service/reader/MultiIndexDocReader.h"
#include "build_service/reader/MultiKVDocReader.h"
#include "build_service/reader/ReaderConfig.h"
#include "build_service/reader/SwiftProcessedDocReader.h"
#include "build_service/reader/SwiftProcessedDocReaderV2.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/reader/SwiftTopicStreamRawDocumentReader.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/util/metrics/MetricProvider.h"


using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, RawDocumentReaderCreator);

RawDocumentReaderCreator::RawDocumentReaderCreator(const SwiftClientCreatorPtr& swiftClientCreator,
                                                   std::shared_ptr<HologresInterface> hologresInterface)
    : _swiftClientCreator(swiftClientCreator)
    , _hologresInterface(hologresInterface)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

RawDocumentReaderCreator::~RawDocumentReaderCreator() {}

RawDocumentReader* RawDocumentReaderCreator::create(const config::ResourceReaderPtr& resourceReader,
                                                    const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
                                                    indexlib::util::MetricProviderPtr metricProvider,
                                                    const indexlib::util::CounterMapPtr& counterMap,
                                                    const indexlib::config::IndexPartitionSchemaPtr schema)
{
    return createSingleSourceReader(resourceReader, kvMap, partitionId, metricProvider, counterMap, schema, nullptr);
}

RawDocumentReader* RawDocumentReaderCreator::create(const config::ResourceReaderPtr& resourceReader,
                                                    const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
                                                    indexlib::util::MetricProviderPtr metricProvider,
                                                    const indexlib::util::CounterMapPtr& counterMap,
                                                    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    return createSingleSourceReader(resourceReader, kvMap, partitionId, metricProvider, counterMap, nullptr, schema);
}

RawDocumentReader* RawDocumentReaderCreator::createSingleSourceReader(
    const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
    indexlib::util::MetricProviderPtr metricProvider, const indexlib::util::CounterMapPtr& counterMap,
    const indexlib::config::IndexPartitionSchemaPtr schema,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schemaV2)
{
    ReaderInitParam param;
    param.kvMap = kvMap;
    param.resourceReader = resourceReader;
    param.range = partitionId.range();
    param.metricProvider = metricProvider;
    param.counterMap = counterMap;
    param.schema = schema;
    param.schemaV2 = schemaV2;
    param.partitionId = partitionId;
    RawDocumentReader* reader = NULL;
    string sourceType = getValueFromKeyValueMap(kvMap, READ_SRC_TYPE);
    if (FILE_READ_SRC == sourceType) {
        string metaDir;
        if (FileListCollector::getBSRawFileMetaDir(resourceReader.get(), partitionId.buildid().datatable(),
                                                   partitionId.buildid().generationid(), metaDir)) {
            param.kvMap[BS_RAW_FILE_META_DIR] = metaDir;
        }
        reader = new FileRawDocumentReader();
    } else if (FIX_LEN_BINARY_FILE_READ_SRC == sourceType) {
        reader = new BinaryFileRawDocumentReader(true);
    } else if (VAR_LEN_BINARY_FILE_READ_SRC == sourceType) {
        reader = new BinaryFileRawDocumentReader(false);
    } else if (INDEX_READ_SRC == sourceType) {
        reader = new IndexDocReader();
    } else if (INDEX_DOCUMENT_READ_SRC == sourceType) {
        reader = new MultiIndexDocReader();
    } else if (KV_DOCUMENT_READ_SRC == sourceType) {
        reader = new MultiKVDocReader();
    } else if (INDEX_TO_FILE_READ_SRC == sourceType) {
        reader = new IndexDocToFileReader();
    } else if (SWIFT_PROCESSED_READ_SRC == sourceType) {
        if (param.schemaV2) {
            reader = new SwiftProcessedDocReaderV2(_swiftClientCreator);
        } else {
            reader = new SwiftProcessedDocReader(_swiftClientCreator);
        }
    } else if (SWIFT_READ_SRC == sourceType) {
        bool topicStreamMode = getValueFromKeyValueMap(kvMap, SWIFT_TOPIC_STREAM_MODE) == "true";
        if (topicStreamMode) {
            reader = new SwiftTopicStreamRawDocumentReader(_swiftClientCreator);
        } else {
            reader = new SwiftRawDocumentReader(_swiftClientCreator);
        }
    } else if (PLUGIN_READ_SRC == sourceType) {
        string moduleName = getValueFromKeyValueMap(kvMap, READ_SRC_MODULE_NAME);
        string modulePath = getValueFromKeyValueMap(kvMap, READ_SRC_MODULE_PATH);
        plugin::ModuleInfo moduleInfo;
        moduleInfo.moduleName = moduleName;
        moduleInfo.modulePath = modulePath;
        if (!_readerManagerPtr) {
            ReaderManagerPtr managerPtr(new ReaderManager(resourceReader));
            if (!managerPtr->init(ReaderConfig())) {
                string errorMsg = "ReaderManager init failed";
                REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
                return NULL;
            }
            _readerManagerPtr = managerPtr;
        }
        reader = _readerManagerPtr->getRawDocumentReaderByModule(moduleInfo);
        if (!reader) {
            string errorMsg = "load reader plugin failed";
            REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
            return NULL;
        }
    }
    else {
        string errorMsg = "Unsupported data source type[" + sourceType + "].";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
        return NULL;
    }
    reader->initBeeperTagsFromPartitionId(partitionId);

    if (!reader->initialize(param)) {
        std::vector<ErrorInfo> errorInfos;
        reader->fillErrorInfos(errorInfos);
        addErrorInfos(errorInfos);
        if (errorInfos.empty()) {
            string errorMsg = "RawDocumentReader [type:" + sourceType + "] initialize fail";
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        }
        delete reader;
        return NULL;
    }
    return reader;
}

}} // namespace build_service::reader
