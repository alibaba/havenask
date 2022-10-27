#include <ha3_sdk/testlib/builder/BuilderTestHelper.h>
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);

using namespace build_service::document;

namespace build_service {
namespace processor {

HA3_LOG_SETUP(builder, BuilderTestHelper);

BuilderTestHelper::BuilderTestHelper() { 
}

BuilderTestHelper::~BuilderTestHelper() { 
}

RawDocumentPtr BuilderTestHelper::makeRawDocument(const string &kvStrs) {
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument);
    vector<string> kvPairs;
    StringUtil::fromString(kvStrs, kvPairs, ";");
    for (size_t i = 0; i < kvPairs.size(); i++) {
        const string &oneField = kvPairs[i];
        vector<string> kvVec;
        StringUtil::fromString(oneField, kvVec, "=");
        if (kvVec.size() != 2) {
            continue;
        }
        rawDoc->setField(kvVec[0], kvVec[1]);
    }
    return rawDoc;
}

ExtendDocumentPtr BuilderTestHelper::makeDocument(const string &kvStrs) {
    ExtendDocumentPtr extendDoc(new ExtendDocument);
    RawDocumentPtr rawDoc = makeRawDocument(kvStrs);
    extendDoc->setRawDocument(rawDoc);
    return extendDoc;
}

void BuilderTestHelper::prepareKeyValueMap(const string &inputStr, 
        KeyValueMap &kvMap)
{
    vector<vector<string> > strVecs;
    StringUtil::fromString(inputStr, strVecs, "=", ";");
    for (size_t i = 0; i < strVecs.size(); i++) {
        assert(strVecs[i].size() == 2);
        kvMap[strVecs[i][0]] = strVecs[i][1];
    }
}

IndexPartitionSchemaPtr BuilderTestHelper::makeSchema(
        const string &fieldConfigs, 
        const string &indexConfigs, 
        const string &attributeConfigs, 
        const string &summaryConfigs)
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema, fieldConfigs, 
            indexConfigs, attributeConfigs, summaryConfigs);
    return schema;
}

};
};

