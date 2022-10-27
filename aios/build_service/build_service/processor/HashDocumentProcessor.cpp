#include "build_service/processor/HashDocumentProcessor.h"
#include <autil/HashFuncFactory.h>
#include "build_service/config/ResourceReader.h"
#include <indexlib/indexlib.h>
#include <indexlib/util/counter/accumulative_counter.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, HashDocumentProcessor);

const string HashDocumentProcessor::PROCESSOR_NAME = "HashDocumentProcessor";

HashDocumentProcessor::HashDocumentProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

HashDocumentProcessor::~HashDocumentProcessor() {
}

bool HashDocumentProcessor::init(const DocProcessorInitParam &param) {
    ResourceReader *resourceReader = param.resourceReader;
    _schema = param.schemaPtr;
    _clusterNames = param.clusterNames;
    // notice: if configurate multi cluster, they must share the same schema
    for (const auto& clusterName : param.clusterNames) {
        string relativePath = ResourceReader::getClusterConfRelativePath(clusterName);
        HashModeConfig hashModeConfig;
        if (!hashModeConfig.init(clusterName, resourceReader, _schema)) {
            BS_LOG(ERROR, "create hashModeConfig failed");
            return false;
        }
        ClusterHashMeta meta;
        if (!createClusterHashMeta(hashModeConfig, meta)) {
            BS_LOG(ERROR, "create clusterHashMeta failed");
            return false;            
        }
        _clusterHashMetas.push_back(meta);
    }

    if (!param.processForSubDoc) {
        _hashFieldErrorCounter = GET_ACC_COUNTER(param.counterMap, processor.hashFieldEmptyError);
        if (!_hashFieldErrorCounter) {
            BS_LOG(ERROR, "create attributeConvertErrorCounter failed");
        }        
    }
    return true;
}

bool HashDocumentProcessor::createClusterHashMeta(
        const HashModeConfig &hashModeConfig, ClusterHashMeta &clusterHashMeta)
{
    for (size_t regionId = 0; regionId < _schema->GetRegionCount(); regionId++)
    {
        string regionName = _schema->GetRegionSchema(regionId)->GetRegionName();
        HashMode hashMode;
        if (!hashModeConfig.getHashMode(regionName, hashMode)) {
            BS_LOG(ERROR, "get HashMode failed for region [%s]", regionName.c_str());
            return false;
        }
        auto hashFuncPtr = HashFuncFactory::createHashFunc(hashMode._hashFunction, hashMode._hashParams);
        if (!hashFuncPtr) {
            string errorMsg = "invalid hash function[" + hashMode._hashFunction
                              + "] for cluster[" + hashModeConfig.getClusterName()
                              + "] region_name[" + regionName + "].";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        clusterHashMeta.appendHashInfo(hashFuncPtr, hashMode._hashFields);
    }
    clusterHashMeta.clusterName = hashModeConfig.getClusterName();
    return true;
}

void HashDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool HashDocumentProcessor::process(const ExtendDocumentPtr &document) {
    const ProcessedDocumentPtr &processedDocPtr = document->getProcessedDocument();
    const RawDocumentPtr &rawDocPtr = document->getRawDocument();
    //TODO add validate code in region_processor
    regionid_t regionId = document->getRegionId();
    if (regionId < 0 || regionId >= (regionid_t)_schema->GetRegionCount())
    {
        BS_LOG(ERROR, "invalid regionId [%d]", regionId);
        return false;
    }
    vector<string> hashFieldValues;
    for (auto& clusterHashMeta : _clusterHashMetas)
    {
        hashFieldValues.clear();
        HashFunctionBasePtr hashFuncBase = clusterHashMeta.getHashFunc(regionId);
        const set<string> &canEmptyFields = hashFuncBase->getCanEmptyFields();
        const vector<string>& hashFields = clusterHashMeta.getHashField(regionId);
        bool hasEmptyHashField = false;
        for (size_t i = 0; i < hashFields.size() ; i++) {
            const string &value = rawDocPtr->getField(hashFields[i]);
            if (value == EMPTY_STRING && canEmptyFields.count(hashFields[i]) == 0) {
                hasEmptyHashField = true;
            }
            hashFieldValues.push_back(value);
        }
        if (hasEmptyHashField) {
            document->setWarningFlag(ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY);
            if (_hashFieldErrorCounter) {
                _hashFieldErrorCounter->Increase(1);
            }
        }
        ProcessedDocument::DocClusterMeta meta;
        meta.clusterName = clusterHashMeta.clusterName;
        meta.hashId = hashFuncBase->getHashId(hashFieldValues);
        processedDocPtr->addDocClusterMeta(meta);
    }
    return true;
}

void HashDocumentProcessor::destroy() {
    delete this;
}

DocumentProcessor* HashDocumentProcessor::clone() {
    return new HashDocumentProcessor(*this);
}

}
}
