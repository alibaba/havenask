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
#include "build_service/processor/DocumentProcessorChainCreatorV2.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/plugin/Module.h"
#include "build_service/processor/BuildInDocProcessorFactory.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/processor/MainSubDocProcessorChain.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"
#include "indexlib/document/normal/rewriter/AddToUpdateDocumentRewriter.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/index_base/schema_adapter.h"

using namespace std;
using namespace autil;
using namespace build_service::analyzer;
using namespace build_service::plugin;
using namespace build_service::config;
using namespace indexlib;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, DocumentProcessorChainCreatorV2);

DocumentProcessorChainCreatorV2::DocumentProcessorChainCreatorV2() {}

DocumentProcessorChainCreatorV2::~DocumentProcessorChainCreatorV2() {}

bool DocumentProcessorChainCreatorV2::init(const config::ResourceReaderPtr& resourceReaderPtr,
                                           indexlib::util::MetricProviderPtr metricProvider,
                                           const indexlib::util::CounterMapPtr& counterMap)
{
    _metricProvider = metricProvider;
    _resourceReaderPtr = resourceReaderPtr;
    if (!counterMap) {
        BS_LOG(WARN, "counterMap is NULL");
    }

    _counterMap = counterMap;
    _analyzerFactoryPtr = AnalyzerFactory::create(resourceReaderPtr);
    if (!_analyzerFactoryPtr) {
        BS_LOG(WARN, "analyzer factory is NULL");
    }
    return true;
}

DocumentProcessorChainPtr
DocumentProcessorChainCreatorV2::create(const DocProcessorChainConfig& docProcessorChainConfig,
                                        const vector<string>& clusterNames) const
{
    string tableName = checkAndGetTableName(clusterNames);
    if (tableName.empty()) {
        return DocumentProcessorChainPtr();
    }

    auto schema = _resourceReaderPtr->getTabletSchemaBySchemaTableName(tableName);
    if (!schema) {
        string errorMsg = "Get Tablet Schema [" + tableName + "] failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DocumentProcessorChainPtr();
    }
    PlugInManagerPtr plugInManagerPtr(
        new PlugInManager(_resourceReaderPtr, _resourceReaderPtr->getPluginPath(), MODULE_FUNC_BUILDER));
    if (!plugInManagerPtr->addModules(docProcessorChainConfig.moduleInfos)) {
        return DocumentProcessorChainPtr();
    }
    bool needDefaultRegionProcessor = false;

    bool needSubParser = false;
    // bool rawText = schema->GetTableType() == tt_linedata;
    bool rawText = false;
    ProcessorInfos mainProcessorInfos = constructProcessorInfos(docProcessorChainConfig.processorInfos,
                                                                needDefaultRegionProcessor, needSubParser, rawText);

    SingleDocProcessorChain* mainChain =
        createSingleChain(plugInManagerPtr, mainProcessorInfos, schema, clusterNames, false);
    if (!mainChain) {
        return DocumentProcessorChainPtr();
    }

    DocumentProcessorChainPtr retChain;
    retChain.reset(mainChain);

    if (!initDocumentProcessorChain(docProcessorChainConfig, mainProcessorInfos, schema, clusterNames, tableName,
                                    retChain)) {
        return DocumentProcessorChainPtr();
    }
    return retChain;
}

std::unique_ptr<indexlibv2::document::IDocumentFactory>
DocumentProcessorChainCreatorV2::createDocumentFactory(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                       const std::vector<std::string>& clusterNames) const
{
    if (!schema) {
        BS_LOG(ERROR, "tablet schema is nullptr");
        return nullptr;
    }
    const auto& tableType = schema->GetTableType();
    auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        BS_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
               autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType()).c_str());
        return nullptr;
    }
    return tabletFactory->CreateDocumentFactory(schema);
}

bool DocumentProcessorChainCreatorV2::initDocumentProcessorChain(
    const DocProcessorChainConfig& docProcessorChainConfig, const ProcessorInfos& mainProcessorInfos,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, const vector<string>& clusterNames,
    const string& tableName, DocumentProcessorChainPtr retChain) const
{
    DocumentInitParamPtr docInitParam =
        createBuiltInInitParam(schema, clusterNames, needAdd2UpdateRewriter(mainProcessorInfos));
    if (!docInitParam) {
        BS_LOG(ERROR, "create builtin init param fail!");
        return false;
    }
    // const SourceSchemaPtr& sourceSchema = schema->GetSourceSchema();
    SourceSchemaParserFactoryGroupPtr parserFactoryGroup(new SourceSchemaParserFactoryGroup);
    // if (sourceSchema) {
    //     if (!parserFactoryGroup->Init(schema, _resourceReaderPtr->getPluginPath())) {
    //         BS_LOG(ERROR, "init source schema parser factory group failed.");
    //         return false;
    //     }
    // }

    auto documentFactory = createDocumentFactory(schema, clusterNames);
    if (!retChain->initV2(std::move(documentFactory), docInitParam, parserFactoryGroup, schema)) {
        BS_LOG(ERROR, "init document parser fail!");
        return false;
    }
    retChain->setTolerateFieldFormatError(docProcessorChainConfig.tolerateFormatError);
    retChain->setDocumentSerializeVersion(docProcessorChainConfig.indexDocSerializeVersion);
    BS_LOG(INFO, "DocumentProcessorChainCreatorV2 set indexDocSerializeVersion [%u] for table [%s]",
           docProcessorChainConfig.indexDocSerializeVersion, tableName.c_str());
    return true;
}

std::string DocumentProcessorChainCreatorV2::checkAndGetTableName(const std::vector<std::string>& clusterNames) const
{
    bool firstTableName = true;
    string retTableName;
    for (size_t i = 0; i < clusterNames.size(); ++i) {
        string tableName = getTableName(clusterNames[i]);
        if (tableName.empty()) {
            string errorMsg = "can't find table name in cluster[" + clusterNames[i] + "]";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return "";
        }
        if (firstTableName) {
            retTableName = tableName;
            firstTableName = false;
        } else if (tableName != retTableName) {
            string errorMsg = "Clusters[" + autil::legacy::ToJsonString(clusterNames) +
                              "] in one processor chain must has same table name!";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return "";
        }
    }
    return retTableName;
}

std::string DocumentProcessorChainCreatorV2::getTableName(const std::string& clusterName) const
{
    string tableName;
    string clusterFileName = _resourceReaderPtr->getClusterConfRelativePath(clusterName);
    if (!_resourceReaderPtr->getConfigWithJsonPath(clusterFileName, "cluster_config.table_name", tableName)) {
        string errorMsg = "get tableName from cluster_config.table_name failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    return tableName;
}

SingleDocProcessorChain*
DocumentProcessorChainCreatorV2::createSingleChain(const PlugInManagerPtr& plugInManager,
                                                   const ProcessorInfos& processorInfos,
                                                   const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                   const vector<string>& clusterNames, bool processForSubDoc) const
{
    unique_ptr<SingleDocProcessorChain> singleChain(
        new SingleDocProcessorChain(plugInManager, /*IndexPartitionSchemaPtr*/ nullptr, _analyzerFactoryPtr));

    DocProcessorInitParam param;
    param.schemaPtr = schema->GetLegacySchema();
    param.schema = schema;
    param.resourceReader = _resourceReaderPtr.get();
    param.analyzerFactory = _analyzerFactoryPtr.get();
    param.clusterNames = clusterNames;
    param.metricProvider = _metricProvider;
    param.counterMap = _counterMap;
    param.processForSubDoc = processForSubDoc;

    BuildInDocProcessorFactory buildInFactory;

    for (size_t i = 0; i < processorInfos.size(); ++i) {
        const string& moduleName = processorInfos[i].moduleName;
        DocumentProcessorFactory* factory = NULL;
        if (PlugInManager::isBuildInModule(moduleName)) {
            factory = &buildInFactory;
        } else {
            Module* module = plugInManager->getModule(moduleName);
            if (!module) {
                string errorMsg = "Init Document processor chain failed. no module name[" + moduleName + "]";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return NULL;
            }
            factory = dynamic_cast<DocumentProcessorFactory*>(module->getModuleFactory());
            if (!factory) {
                string errorMsg = "Invalid module[" + moduleName + "].";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return NULL;
            }
        }
        param.parameters = processorInfos[i].parameters;
        DocumentProcessor* processor = createDocumentProcessor(factory, processorInfos[i].className, param);
        if (!processor) {
            return NULL;
        }
        singleChain->addDocumentProcessor(processor);
    }

    return singleChain.release();
}

DocumentProcessor* DocumentProcessorChainCreatorV2::createDocumentProcessor(DocumentProcessorFactory* factory,
                                                                            const string& className,
                                                                            const DocProcessorInitParam& param) const
{
    DocumentProcessor* processor = (DocumentProcessor*)factory->createDocumentProcessor(className);
    if (!processor) {
        string errorMsg = "create document processor[" + className + "] from factory failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    if (!processor->init(param)) {
        string errorMsg = "Init document processor[" + className + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        delete processor;
        return NULL;
    }
    return processor;
}

ProcessorInfos DocumentProcessorChainCreatorV2::constructProcessorInfos(const ProcessorInfos& processorInfoInConfig,
                                                                        bool needDefaultRegionProcessor,
                                                                        bool needSubParser, bool rawText)
{
    ProcessorInfos processorInfos = processorInfoInConfig;

    if (needSubParser) {
        insertProcessorInfo(processorInfos, ProcessorInfo("SubDocumentExtractorProcessor"), INSERT_TO_FIRST);
    }
    if (needDefaultRegionProcessor) {
        KeyValueMap params;
        params[REGION_DISPATCHER_CONFIG] = FIELD_DISPATCHER_TYPE;
        params[REGION_FIELD_NAME_CONFIG] = DEFAULT_REGION_FIELD_NAME;
        insertProcessorInfo(processorInfos, ProcessorInfo("RegionDocumentProcessor", "", params), INSERT_TO_FIRST);
    }
    // insert HashDocumentProcessor after RegionDocumentProcessor
    ProcessorInfo defaultHashProcessor("HashDocumentProcessor");
    if (find(processorInfos.begin(), processorInfos.end(), defaultHashProcessor) == processorInfos.end()) {
        bool insertFlag = false;
        for (auto iter = processorInfos.begin(); iter != processorInfos.end(); ++iter) {
            if ((*iter).className == RegionDocumentProcessor::PROCESSOR_NAME) {
                processorInfos.insert(iter + 1, defaultHashProcessor);
                insertFlag = true;
                break;
            }
        }
        if (!insertFlag) {
            processorInfos.insert(processorInfos.begin(), defaultHashProcessor);
        }
    }

    if (rawText) {
        insertProcessorInfo(processorInfos, ProcessorInfo("LineDataDocumentProcessor"), INSERT_TO_LAST);
    }
    // else {
    //     insertProcessorInfo(processorInfos, ProcessorInfo("ClassifiedDocumentProcessor"),
    //                         INSERT_TO_LAST);
    //     insertProcessorInfo(processorInfos, ProcessorInfo("DocumentValidateProcessor"),
    //                         INSERT_TO_LAST);
    // }
    return processorInfos;
}

void DocumentProcessorChainCreatorV2::insertProcessorInfo(ProcessorInfos& processorInfos,
                                                          const ProcessorInfo& insertProcessorInfo,
                                                          ProcessorInfoInsertType type)
{
    if (find(processorInfos.begin(), processorInfos.end(), insertProcessorInfo) != processorInfos.end()) {
        return;
    }
    if (type == INSERT_TO_LAST) {
        processorInfos.push_back(insertProcessorInfo);
    } else {
        processorInfos.insert(processorInfos.begin(), insertProcessorInfo);
    }
}

bool DocumentProcessorChainCreatorV2::needAdd2UpdateRewriter(const config::ProcessorInfos& processorInfos)
{
    for (size_t i = 0; i < processorInfos.size(); i++) {
        if (processorInfos[i].className == MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME) {
            return true;
        }
    }
    return false;
}

bool DocumentProcessorChainCreatorV2::initParamForAdd2UpdateRewriter(
    const shared_ptr<indexlibv2::config::ITabletSchema>& schema, const vector<string>& clusterNames,
    vector<shared_ptr<indexlibv2::config::TruncateOptionConfig>>& optionsVec,
    std::vector<indexlibv2::config::SortDescriptions>& sortDescVec) const
{
    assert(!clusterNames.empty());
    assert(optionsVec.empty());
    assert(sortDescVec.empty());

    optionsVec.resize(clusterNames.size());
    sortDescVec.resize(clusterNames.size());
    vector<shared_ptr<indexlibv2::config::TabletOptions>> tabletOptions(clusterNames.size());
    for (size_t i = 0; i < clusterNames.size(); ++i) {
        BuilderClusterConfig builderClusterConfig;
        if (!builderClusterConfig.init(clusterNames[i], *_resourceReaderPtr) || !builderClusterConfig.validate()) {
            string errorMsg =
                "init createAdd2UpdateRewriter param for cluster[" + clusterNames[i] + "] error, use default param";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            continue;
        }

        const BuilderConfig& builderConfig = builderClusterConfig.builderConfig;
        if (builderConfig.sortBuild) {
            sortDescVec[i] = builderConfig.sortDescriptions;
        }
        tabletOptions[i] = _resourceReaderPtr->getTabletOptions(clusterNames[i]);
    }

    // init truncateIndexConfig from schema
    for (size_t i = 0; i < clusterNames.size(); ++i) {
        if (!tabletOptions[i]) {
            continue;
        }
        const auto* truncateStrategys = std::any_cast<std::vector<indexlibv2::config::TruncateStrategy>>(
            tabletOptions[i]->GetDefaultMergeConfig().GetHookOption(index::TRUNCATE_STRATEGY));
        if (!truncateStrategys) {
            BS_LOG(ERROR, "get truncate strategy from merge config failed");
            return false;
        }
        auto truncOptionConfig = make_shared<indexlibv2::config::TruncateOptionConfig>(*truncateStrategys);
        auto [status, truncateProfileConfigs] =
            schema->GetRuntimeSettings().GetValue<std::vector<indexlibv2::config::TruncateProfileConfig>>(
                "truncate_profile");
        if (!status.IsOK()) {
            if (!status.IsNotFound()) {
                BS_LOG(ERROR, "get truncate profile from setting failed.");
                return false;
            }
            continue;
        }
        truncOptionConfig->Init(schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR),
                                truncateProfileConfigs);
        optionsVec[i] = truncOptionConfig;
    }
    return true;
}

std::shared_ptr<indexlibv2::document::IDocumentRewriter>
DocumentProcessorChainCreatorV2::createAndInitAdd2UpdateRewriter(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, const vector<string>& clusterNames) const
{
    vector<shared_ptr<indexlibv2::config::TruncateOptionConfig>> optionsVec;
    vector<indexlibv2::config::SortDescriptions> sortDescVec;
    if (!initParamForAdd2UpdateRewriter(schema, clusterNames, optionsVec, sortDescVec)) {
        BS_LOG(ERROR, "init param for add2update rewriter failed");
        return nullptr;
    }

    auto add2updateDocRewriter = make_shared<indexlibv2::document::AddToUpdateDocumentRewriter>();
    assert(optionsVec.size() == sortDescVec.size());
    auto status = add2updateDocRewriter->Init(schema, optionsVec, sortDescVec);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "add2update doc rewriter init failed, [%s]", status.ToString().c_str());
        return nullptr;
    }
    return add2updateDocRewriter;
}

DocumentInitParamPtr DocumentProcessorChainCreatorV2::createBuiltInInitParam(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, const vector<string>& clusterNames,
    bool needAdd2Update) const
{
    std::vector<std::shared_ptr<indexlibv2::document::IDocumentRewriter>> documentRewriters;
    if (needAdd2Update) {
        auto docRewriter = createAndInitAdd2UpdateRewriter(schema, clusterNames);
        if (docRewriter) {
            documentRewriters.push_back(docRewriter);
        } else {
            string errorMsg = "create add2update document writer failed for table[" + schema->GetTableName() + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return nullptr;
        }
    }

    indexlibv2::document::BuiltInParserInitResource resource;
    resource.counterMap = _counterMap;
    resource.metricProvider = _metricProvider;
    resource.docRewriters = documentRewriters;

    // TODO: get document parser config from tablet schema
    if (false) {
        return DocumentInitParamPtr(
            new indexlibv2::document::BuiltInParserInitParam(/*docParserConfig->GetParameters()*/ {}, resource));
    }
    DocumentInitParam::KeyValueMap kvMap;
    return DocumentInitParamPtr(new indexlibv2::document::BuiltInParserInitParam(kvMap, resource));
}
}} // namespace build_service::processor
