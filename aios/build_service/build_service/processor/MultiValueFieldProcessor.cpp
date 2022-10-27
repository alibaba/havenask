#include "build_service/processor/MultiValueFieldProcessor.h"
#include <autil/legacy/jsonizable.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
using namespace build_service::analyzer;
using namespace build_service::document;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, MultiValueFieldProcessor);

const string MultiValueFieldProcessor::PROCESSOR_NAME = "MultiValueFieldProcessor";

MultiValueFieldProcessor::MultiValueFieldProcessor() {
}

MultiValueFieldProcessor::~MultiValueFieldProcessor() {
}

bool MultiValueFieldProcessor::init(
        const KeyValueMap &kvMap,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
        config::ResourceReader *resourceReader)
{
    // json string: {
    //   "field1" : ",",
    //   "field2" : ";"
    //}
    string fieldSepDesc = getValueFromKeyValueMap(kvMap, "field_separator_description");
    BS_LOG(INFO, "fieldSepDesc: %s", fieldSepDesc.c_str());
    try {
        FromJsonString(_fieldSepDescription, fieldSepDesc);
    } catch (const autil::legacy::ExceptionBase &e) {
        return false;
    }
    return true;
}

bool MultiValueFieldProcessor::process(const ExtendDocumentPtr &document) {
    assert(document);
    RawDocumentPtr rawDoc = document->getRawDocument();
    for (const auto &it: _fieldSepDescription) {
        const string &fieldName = it.first;
        const string &sep = it.second;
        string fieldValue = rawDoc->getField(fieldName);
        //replace
        auto fieldValueSlices = autil::StringUtil::split(fieldValue, sep);
        fieldValue = autil::StringUtil::toString(fieldValueSlices,
                string(1, MULTI_VALUE_SEPARATOR));
        rawDoc->setField(fieldName, fieldValue);
    }
    return true;
}

void MultiValueFieldProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void MultiValueFieldProcessor::destroy() {
    delete this;
}

DocumentProcessor* MultiValueFieldProcessor::clone() {
    return new MultiValueFieldProcessor(*this);
}


}
}
