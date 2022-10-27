#include "build_service/processor/test/ProcessorTestUtil.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;

using namespace build_service::document;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ProcessorTestUtil);

ProcessorTestUtil::ProcessorTestUtil() {
}

ProcessorTestUtil::~ProcessorTestUtil() {
}


RawDocumentPtr ProcessorTestUtil::createRawDocument(
        const string &fieldMap)
{
    ExtendDocumentPtr extendDoc(new ExtendDocument());
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    vector<string> fields = StringTokenizer::tokenize(ConstString(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = StringTokenizer::tokenize(ConstString(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    rawDoc->setDocOperateType(ADD_DOC);
    return rawDoc;
}

vector<RawDocumentPtr> ProcessorTestUtil::createRawDocuments(
        const string &rawDocStr, const string& sep)
{
    vector<string> rawDocStrVec = StringTokenizer::tokenize(ConstString(rawDocStr), sep);
    vector<RawDocumentPtr> rawDocVec;
    for (const auto& docStr : rawDocStrVec) {
        rawDocVec.push_back(createRawDocument(docStr));
    }
    return rawDocVec;
}

ExtendDocumentPtr ProcessorTestUtil::createExtendDoc(
        const string &fieldMap, regionid_t regionId)
{
    ExtendDocumentPtr extendDoc(new ExtendDocument());
    extendDoc->setRawDocument(createRawDocument(fieldMap));
    extendDoc->setRegionId(regionId);
    return extendDoc;
}


}
}
