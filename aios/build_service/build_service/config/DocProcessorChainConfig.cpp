#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ConfigDefine.h"
#include <indexlib/indexlib.h>

using namespace std;

namespace build_service {
namespace config {

BS_LOG_SETUP(config, DocProcessorChainConfig);

void ProcessorInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("class_name", className);
    json.Jsonize("module_name", moduleName, string());
    json.Jsonize("parameters", parameters, KeyValueMap());
}

////////////////////////////////////////////////////////////////

DocProcessorChainConfig::DocProcessorChainConfig()
    : indexDocSerializeVersion(INVALID_DOC_VERSION)
    , tolerateFormatError(true)
    , useRawDocAsDoc(false)
{
}

DocProcessorChainConfig::~DocProcessorChainConfig() {
}

void DocProcessorChainConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("clusters", clusterNames);
    json.Jsonize("modules", moduleInfos, plugin::ModuleInfos());
    json.Jsonize("document_processor_chain", processorInfos, ProcessorInfos());
    json.Jsonize("sub_document_processor_chain", subProcessorInfos, ProcessorInfos());
    json.Jsonize("tolerate_field_format_error", tolerateFormatError, tolerateFormatError);
    json.Jsonize("use_raw_doc_as_document", useRawDocAsDoc, useRawDocAsDoc);
    json.Jsonize("index_document_serialize_version",
                 indexDocSerializeVersion, indexDocSerializeVersion);
    json.Jsonize("chain_name", chainName, chainName);
}

bool DocProcessorChainConfig::validate() const {
    // hard to validate everything ...
    for (size_t i = 0; i < subProcessorInfos.size(); ++i) {
        string className = subProcessorInfos[i].className;
        if (className == MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME) {
            string errorMsg = "sub document processor chain not support"
                              "ModifiedFieldsDocumentProcessor";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

}
}
