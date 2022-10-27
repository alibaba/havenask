#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/FileRawDocumentReader.h"
#include "build_service/reader/BinaryFileRawDocumentReader.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/SwiftClientCreator.h"
#include <autil/StringUtil.h>
#include "build_service/reader/ReaderConfig.h"
#include <autil/legacy/any.h>
#include "build_service/reader/FileListCollector.h"
#include <indexlib/misc/metric_provider.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
using namespace autil::legacy;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, RawDocumentReaderCreator);

RawDocumentReaderCreator::RawDocumentReaderCreator(
    const SwiftClientCreatorPtr& swiftClientCreator)
    : _swiftClientCreator(swiftClientCreator)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

RawDocumentReaderCreator::~RawDocumentReaderCreator() {
}

RawDocumentReader *RawDocumentReaderCreator::create(
        const config::ResourceReaderPtr &resourceReader,
        const KeyValueMap &kvMap,
        const proto::PartitionId &partitionId,
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
        const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema)
{
    return createSingleSourceReader(resourceReader, kvMap, partitionId, metricProvider, counterMap, schema);
}

RawDocumentReader *RawDocumentReaderCreator::createSingleSourceReader(
        const config::ResourceReaderPtr &resourceReader,
        const KeyValueMap &kvMap,
        const proto::PartitionId &partitionId,
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
        const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema)
{
    ReaderInitParam param;
    param.kvMap = kvMap;
    param.resourceReader = resourceReader;
    param.range = partitionId.range();
    param.metricProvider = metricProvider;
    param.counterMap = counterMap;
    param.schema = schema;
    param.partitionId = partitionId;
    RawDocumentReader *reader = NULL;
    string sourceType = getValueFromKeyValueMap(kvMap, READ_SRC_TYPE);
    if (FILE_READ_SRC == sourceType) {
        string metaDir;
        if (FileListCollector::getBSRawFileMetaDir(
                        resourceReader.get(), partitionId.buildid().datatable(),
                        partitionId.buildid().generationid(), metaDir))
        {
            param.kvMap[BS_RAW_FILE_META_DIR] = metaDir;
        }
        reader = new FileRawDocumentReader();
    } else if (FIX_LEN_BINARY_FILE_READ_SRC == sourceType) {
        reader = new BinaryFileRawDocumentReader(true);
    } else if (VAR_LEN_BINARY_FILE_READ_SRC == sourceType) {
        reader = new BinaryFileRawDocumentReader(false);
    }
    else if (SWIFT_READ_SRC == sourceType) {
        reader = new SwiftRawDocumentReader(_swiftClientCreator);
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
    } else {
        string errorMsg = "Unsupported data source type[" + sourceType + "].";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
        return NULL;
    }
    reader->initBeeperTagsFromPartitionId(partitionId);
    
    if (!reader->initialize(param)) {
        std::vector<ErrorInfo> errorInfos;
        reader->fillErrorInfos(errorInfos);
        addErrorInfos(errorInfos);
        if (errorInfos.empty())
        {
            string errorMsg = "RawDocumentReader [type:" + sourceType + "] initialize fail";
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        }
        delete reader;
        return NULL;
    }
    return reader;
}

}
}
