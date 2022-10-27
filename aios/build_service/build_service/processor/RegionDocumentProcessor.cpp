#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/config/CLIOptionNames.h"
#include <indexlib/indexlib.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

BS_NAMESPACE_USE(config);
BS_NAMESPACE_USE(document);

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, RegionDocumentProcessor);

const string RegionDocumentProcessor::PROCESSOR_NAME = "RegionDocumentProcessor";

RegionDocumentProcessor::RegionDocumentProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _specificRegionId(INVALID_REGIONID)
{
}

RegionDocumentProcessor::~RegionDocumentProcessor() {
}

bool RegionDocumentProcessor::init(const DocProcessorInitParam &param) {
    _schema = param.schemaPtr;
    if (!_schema) {
        BS_LOG(ERROR, "init regionDocumentProcessor failed,"
                       "_schema is NULL");
        return false;
    }
    auto iter = param.parameters.find(REGION_DISPATCHER_CONFIG);
    if (iter == param.parameters.end()) {
        BS_LOG(ERROR, "init RegionDocumentProcessor failed, missing config %s",
               REGION_DISPATCHER_CONFIG.c_str());
        return false;
    }
    if (iter->second == FIELD_DISPATCHER_TYPE) {
        _specificRegionId = INVALID_REGIONID;
        auto iterField = param.parameters.find(REGION_FIELD_NAME_CONFIG);
        if (iterField == param.parameters.end()) {
            BS_LOG(ERROR, "init RegionDocumentProcessor failed, missing config %s",
                   REGION_FIELD_NAME_CONFIG.c_str());
            return false;
        }
        _regionFieldName = iterField->second;
        return true;
    }

    if (iter->second == REGION_ID_DISPATCHER_TYPE) {
        auto iterRid = param.parameters.find(REGION_ID_CONFIG);
        if (iterRid == param.parameters.end()) {
            BS_LOG(ERROR, "init RegionDocumentProcessor failed, missing config %s",
                   REGION_ID_CONFIG.c_str());
            return false; 
        }
        if (!StringUtil::numberFromString<int32_t>(iterRid->second, _specificRegionId)) {
            BS_LOG(ERROR, "init RegionDocumentProcessor failed, invalid regionId config %s",
                   iterRid->second.c_str()); 
            return false; 
        }

        if (_specificRegionId < 0) {
            BS_LOG(ERROR, "init RegionDocumentProcessor failed, invalid regionId config %s",
                   iterRid->second.c_str()); 
            return false; 
        }

        size_t regionCount = _schema->GetRegionCount();
        if ((size_t)_specificRegionId > regionCount) {
            BS_LOG(ERROR, "invalid regionId : %d, should not exceed regionCount: %lu ",
                           _specificRegionId, regionCount);
            return false;
        }
        return true;
    }

    BS_LOG(ERROR, "invalid dispatcher_type : %s", iter->second.c_str());
    return false;
}

bool RegionDocumentProcessor::process(const ExtendDocumentPtr &document) {
    if (_specificRegionId != INVALID_REGIONID) {
        document->setRegionId(_specificRegionId);
        return true;
    }

    const RawDocumentPtr &rawDocPtr = document->getRawDocument();    
    const string& regionField = rawDocPtr->getField(_regionFieldName);
    if (regionField.empty()) {
        BS_LOG(ERROR, "RawDocument missing regionField(%s)",
                       _regionFieldName.c_str());
        return false;
    }
    regionid_t regionId = _schema->GetRegionId(regionField);
    if (regionId == INVALID_REGIONID) {
        BS_LOG(ERROR, "invalid regionName (%s)", regionField.c_str());
        return false;        
    }
    document->setRegionId(regionId);
    return true;
}

void RegionDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}


}
}
