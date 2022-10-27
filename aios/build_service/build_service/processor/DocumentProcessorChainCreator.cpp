#include "build_service/processor/DocumentProcessorChainCreator.h"
#include "build_service/processor/MainSubDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/processor/BuildInDocProcessorFactory.h"
#include "build_service/plugin/Module.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/util/Monitor.h"
#include "build_service/config/CLIOptionNames.h"
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/document/document_rewriter/document_rewriter_creator.h>
#include <autil/legacy/jsonizable.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::analyzer;
using namespace build_service::plugin;
using namespace build_service::config;
using namespace heavenask::indexlib;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, DocumentProcessorChainCreator);

DocumentProcessorChainCreator::DocumentProcessorChainCreator()
{
}

DocumentProcessorChainCreator::~DocumentProcessorChainCreator() {
}

bool DocumentProcessorChainCreator::init(
        const config::ResourceReaderPtr &resourceReaderPtr,
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
        const IE_NAMESPACE(util)::CounterMapPtr &counterMap)
{
    _metricProvider = metricProvider;
    _resourceReaderPtr = resourceReaderPtr;
    if (!counterMap) {
        BS_LOG(ERROR, "create DocumentProcessorChainCreator failed due to counterMap is NULL");
    }
    
    _counterMap = counterMap;
    _analyzerFactoryPtr = AnalyzerFactory::create(resourceReaderPtr);
    return _analyzerFactoryPtr != NULL;
}

DocumentProcessorChainPtr DocumentProcessorChainCreator::create(
        const DocProcessorChainConfig &docProcessorChainConfig,
        const vector<string> &clusterNames) const
{
    string tableName = checkAndGetTableName(clusterNames);
    if (tableName.empty()) {
        return DocumentProcessorChainPtr();
    }

    IndexPartitionSchemaPtr schema =
        _resourceReaderPtr->getSchemaBySchemaTableName(tableName);
    if (!schema) {
        string errorMsg = "Get Index Partition Schema [" + tableName + "] failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DocumentProcessorChainPtr();
    }
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(
                    _resourceReaderPtr, _resourceReaderPtr->getPluginPath(), 
                    MODULE_FUNC_BUILDER));
    if (!plugInManagerPtr->addModules(docProcessorChainConfig.moduleInfos)) {
        return DocumentProcessorChainPtr();
    }
    // create main chain
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    bool needDefaultRegionProcessor = false;
    const auto& inProcessorInfos = docProcessorChainConfig.processorInfos;

    if (schema->GetRegionCount() > 1) {
        needDefaultRegionProcessor = true;
        for (size_t i = 0; i < inProcessorInfos.size(); ++i) {
            if (inProcessorInfos[i].className == RegionDocumentProcessor::PROCESSOR_NAME) {
                needDefaultRegionProcessor = false;
                break;
            }
        }
    }

    bool needSubParser = subSchema != NULL;
    bool rawText = schema->GetTableType() == tt_linedata;
    if (needSubParser && rawText) {
        BS_LOG(ERROR, "both needSubParser and rawText.");
        return DocumentProcessorChainPtr();
    }
    ProcessorInfos mainProcessorInfos = constructProcessorInfos(
            docProcessorChainConfig.processorInfos,
            needDefaultRegionProcessor, needSubParser, rawText);

    // check processor order in mainProcessorChain
    if (schema->GetRegionCount() > 1) {
        size_t regionProcessorIdx = mainProcessorInfos.size();
        for (size_t i = 0; i < mainProcessorInfos.size(); ++i) {
            if (mainProcessorInfos[i].className
                == RegionDocumentProcessor::PROCESSOR_NAME)
            {
                regionProcessorIdx = i;
                continue;
            }
            if (mainProcessorInfos[i].className
                == HashDocumentProcessor::PROCESSOR_NAME && i < regionProcessorIdx)
            {
                BS_LOG(ERROR, "RegionDocumentProcessor should be"
                        " configured before HashDocumentProcessor");
                return DocumentProcessorChainPtr();
            }
        }
    }
    
    SingleDocProcessorChain *mainChain = createSingleChain(
        plugInManagerPtr, mainProcessorInfos,
        schema, clusterNames, false);
    if (!mainChain) {
        return DocumentProcessorChainPtr();
    }

    DocumentProcessorChainPtr retChain;
    if (!subSchema) {
        retChain.reset(mainChain);
    } else {
        // create sub chain
        ProcessorInfos subProcessorInfos = constructProcessorInfos(
                docProcessorChainConfig.subProcessorInfos, false, false, false);
        SingleDocProcessorChain *subChain = createSingleChain(
            plugInManagerPtr, subProcessorInfos, subSchema, clusterNames, true);
        if (!subChain) {
            delete mainChain;
            return DocumentProcessorChainPtr();
        }
        retChain.reset(new MainSubDocProcessorChain(mainChain, subChain));
    }

    if (schema) {
        BS_LOG(INFO, "DocumentProcessorChainCreator init DocumentFactoryWrapper "
               "by using schema_name [%s]", schema->GetSchemaName().c_str());
    }
    DocumentFactoryWrapperPtr docFactoryWrapper =
        DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
            schema, CUSTOMIZED_DOCUMENT_CONFIG_PARSER,
            _resourceReaderPtr->getPluginPath());

    if (!docFactoryWrapper) {
        BS_LOG(ERROR, "create document factory wrapper fail!");
        return DocumentProcessorChainPtr();
    }
    DocumentInitParamPtr docInitParam = createBuiltInInitParam(
            schema, clusterNames, needAdd2UpdateRewriter(mainProcessorInfos));
    if (!docInitParam) {
        BS_LOG(ERROR, "create builtin init param fail!");
        return DocumentProcessorChainPtr();
    }
    if (!retChain->init(docFactoryWrapper, docInitParam,
                        docProcessorChainConfig.useRawDocAsDoc))
    {
        BS_LOG(ERROR, "init document parser fail!");
        return DocumentProcessorChainPtr();
    }
    retChain->setTolerateFieldFormatError(docProcessorChainConfig.tolerateFormatError);
    retChain->setDocumentSerializeVersion(docProcessorChainConfig.indexDocSerializeVersion);
    BS_LOG(INFO, "DocumentProcessorChainCreator set indexDocSerializeVersion [%u] for table [%s]",
           docProcessorChainConfig.indexDocSerializeVersion, tableName.c_str());
    return retChain;
}

std::string DocumentProcessorChainCreator::checkAndGetTableName(
        const std::vector<std::string> &clusterNames) const
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
            string errorMsg = "Clusters[" + autil::legacy::ToJsonString(clusterNames)
                              + "] in one processor chain must has same table name!";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return "";
        }
    }
    return retTableName;
}

std::string DocumentProcessorChainCreator::getTableName(
        const std::string &clusterName) const
{
    string tableName;
    string clusterFileName = _resourceReaderPtr->getClusterConfRelativePath(clusterName);
    if (!_resourceReaderPtr->getConfigWithJsonPath(clusterFileName,
                    "cluster_config.table_name", tableName))
    {
        string errorMsg = "get tableName from cluster_config.table_name failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    return tableName;
}

SingleDocProcessorChain *DocumentProcessorChainCreator::createSingleChain(
        const PlugInManagerPtr &plugInManager,
        const ProcessorInfos &processorInfos,
        const IndexPartitionSchemaPtr &schema,
        const vector<string> &clusterNames,
        bool processForSubDoc) const
{
    unique_ptr<SingleDocProcessorChain> singleChain(new SingleDocProcessorChain(plugInManager));

    DocProcessorInitParam param;
    param.schemaPtr = schema;
    param.resourceReader = _resourceReaderPtr.get();
    param.analyzerFactoryPtr = _analyzerFactoryPtr;
    param.clusterNames = clusterNames;
    param.metricProvider = _metricProvider;
    param.counterMap = _counterMap;
    param.processForSubDoc = processForSubDoc; 

    BuildInDocProcessorFactory buildInFactory;

    for (size_t i = 0; i < processorInfos.size(); ++i) {
        const string &moduleName = processorInfos[i].moduleName;
        DocumentProcessorFactory *factory = NULL;
        if (PlugInManager::isBuildInModule(moduleName)) {
            factory = &buildInFactory;
        } else {
            Module* module = plugInManager->getModule(moduleName);
            if (!module) {
                string errorMsg = "Init Document processor chain failed. no module name["
                                  + moduleName + "]";
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
        DocumentProcessor* processor = createDocumentProcessor(
                factory, processorInfos[i].className, param);
        if (!processor) {
            return NULL;
        }
        singleChain->addDocumentProcessor(processor);
    }

    return singleChain.release();
}

DocumentProcessor *DocumentProcessorChainCreator::createDocumentProcessor(
        DocumentProcessorFactory *factory, const string &className,
        const DocProcessorInitParam &param) const
{
    DocumentProcessor *processor = (DocumentProcessor*)factory->createDocumentProcessor(className);
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

ProcessorInfos DocumentProcessorChainCreator::constructProcessorInfos(
        const ProcessorInfos &processorInfoInConfig,
        bool needDefaultRegionProcessor, bool needSubParser, bool rawText)
{
    ProcessorInfos processorInfos = processorInfoInConfig;

    if (needSubParser) {
        insertProcessorInfo(processorInfos, ProcessorInfo("SubDocumentExtractorProcessor"),
                            INSERT_TO_FIRST);
    }
    if (needDefaultRegionProcessor) {
        KeyValueMap params;
        params[REGION_DISPATCHER_CONFIG] = FIELD_DISPATCHER_TYPE;
        params[REGION_FIELD_NAME_CONFIG] = DEFAULT_REGION_FIELD_NAME;        
        insertProcessorInfo(processorInfos, ProcessorInfo("RegionDocumentProcessor", "", params),
                            INSERT_TO_FIRST); 
    }
    // insert HashDocumentProcessor after RegionDocumentProcessor
    ProcessorInfo defaultHashProcessor("HashDocumentProcessor");
    if (find(processorInfos.begin(), processorInfos.end(), defaultHashProcessor)
        == processorInfos.end())
    {
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
        insertProcessorInfo(processorInfos, ProcessorInfo("LineDataDocumentProcessor"),
                            INSERT_TO_LAST);
    }
    // else {
    //     insertProcessorInfo(processorInfos, ProcessorInfo("ClassifiedDocumentProcessor"),
    //                         INSERT_TO_LAST);
    //     insertProcessorInfo(processorInfos, ProcessorInfo("DocumentValidateProcessor"),
    //                         INSERT_TO_LAST);
    // }
    return processorInfos;
}

void DocumentProcessorChainCreator::insertProcessorInfo(
        ProcessorInfos &processorInfos, const ProcessorInfo &insertProcessorInfo,
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

bool DocumentProcessorChainCreator::needAdd2UpdateRewriter(
        const config::ProcessorInfos &processorInfos)
{
    for (size_t i = 0; i < processorInfos.size(); i++)
    {
        if (processorInfos[i].className == 
            MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME)
        {
            return true;
        }
    }
    return false;
}

void DocumentProcessorChainCreator::initParamForAdd2UpdateRewriter(
        const IndexPartitionSchemaPtr &schema,
        const vector<string> &clusterNames,
        vector<IndexPartitionOptions> &optionsVec,
        std::vector<SortDescriptions> &sortDescVec) const
{
    assert(!clusterNames.empty());
    assert(optionsVec.empty());
    assert(sortDescVec.empty());

    optionsVec.resize(clusterNames.size());
    sortDescVec.resize(clusterNames.size());
    for (size_t i = 0; i < clusterNames.size(); ++i)
    {
        BuilderClusterConfig builderClusterConfig;
        if (!builderClusterConfig.init(clusterNames[i], *_resourceReaderPtr)
            || !builderClusterConfig.validate())
        {
            string errorMsg = "init createAdd2UpdateRewriter param for cluster[" 
                              + clusterNames[i] + "] error, use default param";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            continue;
        }

        optionsVec[i] = builderClusterConfig.indexOptions;
        const BuilderConfig& builderConfig = builderClusterConfig.builderConfig;
        if (builderConfig.sortBuild)
        {
            sortDescVec[i] = builderConfig.sortDescriptions;
        }
    }

    // init truncateIndexConfig from schema
    for (size_t i = 0; i < clusterNames.size(); ++i)
    {
        TruncateOptionConfigPtr truncOptionConfig = 
            optionsVec[i].GetMergeConfig().truncateOptionConfig;
        if (truncOptionConfig)
        {
            truncOptionConfig->Init(schema);
        }
    }
}
 
DocumentRewriterPtr DocumentProcessorChainCreator::createAdd2UpdateRewriter(
        const IndexPartitionSchemaPtr &schema,
        const vector<string> &clusterNames) const
{
    vector<IndexPartitionOptions> optionsVec;
    vector<SortDescriptions> sortDescVec;
    initParamForAdd2UpdateRewriter(schema, clusterNames, optionsVec, sortDescVec);

    DocumentRewriterPtr add2updateDocRewriter(
            DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(
                    schema, optionsVec, sortDescVec));
    return add2updateDocRewriter;
}

DocumentInitParamPtr DocumentProcessorChainCreator::createBuiltInInitParam(
        const IndexPartitionSchemaPtr &schema,
        const vector<string> &clusterNames, bool needAdd2Update) const
{
    CustomizedConfigPtr docParserConfig =
        CustomizedConfigHelper::FindCustomizedConfig(
                schema->GetCustomizedDocumentConfigs(), CUSTOMIZED_DOCUMENT_CONFIG_PARSER); 

    DocumentProcessorChain::IndexDocumentRewriterVector documentRewriters;
    if (needAdd2Update) {
        DocumentRewriterPtr docRewriter = createAdd2UpdateRewriter(
                schema, clusterNames);
        if (docRewriter) {
            documentRewriters.push_back(docRewriter);
        } else {
            string errorMsg = 
                "create add2update document writer failed for table[" + schema->GetSchemaName() + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return DocumentInitParamPtr();
        }
    }

    BuiltInParserInitResource resource;
    resource.counterMap = _counterMap;
    resource.metricProvider = _metricProvider;
    resource.docRewriters = documentRewriters;

    if (docParserConfig)
    {
        return DocumentInitParamPtr(new BuiltInParserInitParam(
                        docParserConfig->GetParameters(), resource));
    }
    DocumentInitParam::KeyValueMap kvMap;
    return DocumentInitParamPtr(new BuiltInParserInitParam(kvMap, resource));
}

}
}
