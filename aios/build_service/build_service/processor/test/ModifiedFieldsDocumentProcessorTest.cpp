#include "build_service/test/unittest.h"
#include "build_service/processor/ModifiedFieldsDocumentProcessor.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(config);

using namespace build_service::document;

namespace build_service {
namespace processor {

class ModifiedFieldsDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    typedef IE_NAMESPACE(config)::IndexPartitionSchemaPtr IndexPartitionSchemaPtr;

protected:
    void checkParseIgnoreFields(const IndexPartitionSchemaPtr &schema,
                                const char *msg, std::string kvIgnoreStr,
                                std::string expectIgnoreFields,
                                bool parseSuccess = true);

    void checkDoIgnoreFields(const char *msg,
                             std::string inputModefiedFieldsSetStr,
                             std::string expectModefiedFieldsSet);

    void doTestAddToSkip(const IndexPartitionSchemaPtr &schema);

    void checkAddToSkip(const IndexPartitionSchemaPtr &schema,
                        const char *msg, const char *modifyFields,
                        std::string cmd, std::string expectCmd,
                        std::string kvToUpdateStr,
                        std::string kvIgnoreStr = "");
    void checkSimpleProcess(const char *msg, std::string kvMapStr,
                            std::string modifyFields,
                            std::string expectFieldsStr);

    void checkMainOnly();

    // relationRulesString  : "src1:dst1,dst2,...;src2:dst3;..."
    // ignoreRulesString    : "ignore1,ignore2,..."
    // docString            : "m1,m2,s1,s2,s3,...;s1,s2^^s3"
    // expectModifiedFields : "m1,m2...;s1,s2,s3,..;s1,s2^^s3"
    bool checkProcess(const std::string &relationRulesString,
                      const std::string &ignoreRulesString,
                      const std::string &docString,
                      const std::string &expectModifiedFields,
                      bool expectSkip = false);
    ExtendDocumentPtr createDocument(const std::string &docString);
    bool checkResult(const IndexPartitionSchemaPtr &schema,
                     const ExtendDocumentPtr &doc,
                     const std::string &expectModifiedFields,
                     bool expectSkip);
    bool checkModifiedFields(
            const IndexPartitionSchemaPtr &schema,
            const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &modifiedFieldSet,
            const std::string &expectModifiedFields);


protected:
    IndexPartitionSchemaPtr readIndexSchema(const std::string &schemaPath);
    IndexPartitionSchemaPtr _schemaPtr;
    IndexPartitionSchemaPtr _schemaWithSubSchema;
};

void ModifiedFieldsDocumentProcessorTest::setUp() {
    string configRoot = TEST_DATA_PATH"modified_fields_document_processor_test";
    string schemaFile = "modified_fields_document_processor_schema.json";
    string schemaPath = configRoot + "/" + schemaFile;
    _schemaPtr = readIndexSchema(schemaPath);

    schemaFile = "modified_fields_document_processor_with_sub_schema.json";
    schemaPath = configRoot + "/" + schemaFile;
    _schemaWithSubSchema = readIndexSchema(schemaPath);
}

void ModifiedFieldsDocumentProcessorTest::tearDown() {
}

IndexPartitionSchemaPtr ModifiedFieldsDocumentProcessorTest::readIndexSchema(const string &schemaPath) {
    ifstream fin(schemaPath.c_str());
    assert(fin);

    string jsonStr;
    string line;
    while (getline(fin, line))
    {
        jsonStr.append(line);
    }

    Any any = ParseJson(jsonStr);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    FromJson(*schema, any);

    return schema;
}

void ModifiedFieldsDocumentProcessorTest::checkParseIgnoreFields(
        const IndexPartitionSchemaPtr &schema, const char *msg,
        string kvIgnoreStr, string expectIgnoreFields, bool parseSuccess)
{
    KeyValueMap kvMap;
    kvMap["ignore_fields"] = kvIgnoreStr;

    RawDocumentPtr rawDocPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDocPtr->setField(HA_RESERVED_MODIFY_FIELDS, string());

    ModifiedFieldsDocumentProcessor processor;
    DocProcessorInitParam param;
    param.parameters = kvMap;
    param.schemaPtr = schema;
    ASSERT_EQ(parseSuccess, processor.init(param)) << msg;
    if (!parseSuccess) {
        return;
    }

    StringTokenizer st(expectIgnoreFields, ",",
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ(st.getNumTokens(), processor._ModifiedFieldsModifierVec.size()) << msg;
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        ASSERT_EQ(fieldSchema->GetFieldId(st[i]), 
                  processor._ModifiedFieldsModifierVec[i]->GetSrcFieldId()) << msg;
    }
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testInitIgnoreFields) {
    checkParseIgnoreFields(_schemaPtr,
            "sub:1", "auction_type,user_id,category",
            "auction_type,user_id,category");
    checkParseIgnoreFields(_schemaPtr,
                           "sub:5", "auction_type,,user_id, ,,",
                           "auction_type,user_id");
    checkParseIgnoreFields(_schemaPtr,
                           "sub:6", "auction_type,user_id,useless_fields",
                           "auction_type,user_id", false);
    //TODO: optimize
    checkParseIgnoreFields(_schemaPtr,
                           "sub:7", "auction_type,auction_type,user_id",
                           "auction_type,auction_type,user_id");
    checkParseIgnoreFields(_schemaPtr,
                           "sub:8", ":", "", false);
    checkParseIgnoreFields(_schemaPtr, "sub:9", "", "");

    checkParseIgnoreFields(_schemaPtr,
            "sub:11", "auction_type,user_id,category, pk", "", false);

    checkParseIgnoreFields(_schemaWithSubSchema,
            "sub:12", "auction_type,user_id,category, sub_pk", "", false);
}

void ModifiedFieldsDocumentProcessorTest::checkDoIgnoreFields(
        const char *msg, string inputModefiedFieldsSetStr,
        string expectFieldsStr)
{
    RawDocumentPtr rawDocPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDocPtr->setField(CMD_TAG, CMD_ADD);
    rawDocPtr->setField(HA_RESERVED_MODIFY_FIELDS, inputModefiedFieldsSetStr);

    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDocPtr);

    ModifiedFieldsDocumentProcessor processor;
    DocProcessorInitParam param;
    param.schemaPtr = _schemaPtr;
    ASSERT_TRUE(processor.init(param)) << msg;

    ASSERT_TRUE(processor.process(extendDoc));

    const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &modifiedFieldSet =
        extendDoc->getIndexExtendDoc()->getModifiedFieldSet();
    StringTokenizer st(expectFieldsStr, ",",
            StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ(st.getNumTokens(), modifiedFieldSet.size()) << msg;
    for (StringTokenizer::Iterator it = st.begin();
         it != st.end(); ++it)
    {
        fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId(*it);
        ASSERT_TRUE(modifiedFieldSet.find(fieldId) != modifiedFieldSet.end()) << msg;
    }
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testDoIgnoreFields) {
    checkDoIgnoreFields("sub:1","useless_field1,useless_field2","");
    checkDoIgnoreFields("sub:2","useless_field1,fF","");
}

void ModifiedFieldsDocumentProcessorTest::checkAddToSkip(
        const IndexPartitionSchemaPtr &schema, const char *msg,
        const char *modifyFields, string cmd,
        string expectCmd, string kvToUpdateStr,
        string kvIgnoreStr)
{
    KeyValueMap kvMap;
    if (!kvIgnoreStr.empty()) {
        kvMap["ignore_fields"] = kvIgnoreStr;
        kvMap["rewrite_add_to_update"] = kvToUpdateStr;
    }
    RawDocumentPtr rawDocPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDocPtr->setField(CMD_TAG, cmd);
    rawDocPtr->setField("title", "wanggf");
    rawDocPtr->setField("user_id", "wanggaofei");
    rawDocPtr->setField("useless_fields", "useless");
    rawDocPtr->setField("price", "1.23");
    if (modifyFields) {
        rawDocPtr->setField(HA_RESERVED_MODIFY_FIELDS, modifyFields);
    }

    DocOperateType expectDocOp =
        IE_NAMESPACE(document)::DefaultRawDocument::getDocOperateType(ConstString(expectCmd));

    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDocPtr);

    ModifiedFieldsDocumentProcessor processor;
    DocProcessorInitParam param;
    param.parameters = kvMap;
    param.schemaPtr = schema;
    ASSERT_TRUE(processor.init(param)) << msg;
    ASSERT_TRUE(processor.process(extendDoc)) << msg; 
    ASSERT_EQ(expectDocOp, rawDocPtr->getDocOperateType()) << msg;
}

void ModifiedFieldsDocumentProcessorTest::doTestAddToSkip(
        const IndexPartitionSchemaPtr &schema)
{
    checkAddToSkip(schema, "sub:1", NULL, CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:1_2", NULL, CMD_ADD, CMD_ADD, "false");
    checkAddToSkip(schema, "sub:2", "", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:3", " ", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:4", ";;", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:5", "title", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:6", "auction_type", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:7", "useless_fields", CMD_ADD, CMD_SKIP, "true");
    checkAddToSkip(schema, "sub:7_2", "useless_fields", CMD_ADD, CMD_SKIP, "false");
    checkAddToSkip(schema, "sub:8", "useless_fields1;useless_fields2", CMD_ADD, CMD_SKIP, "true");
    checkAddToSkip(schema, "sub:8_2", "useless_fields1;useless_fields2", CMD_ADD, CMD_SKIP, "false");
    checkAddToSkip(schema, "sub:9", "user_id;category", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:10", "useless_fields;category", CMD_ADD, CMD_ADD, "true");

    // checkAddToSkip(schema, "sub:11", "user_id", CMD_DELETE, CMD_DELETE, "true");
    // checkAddToSkip(schema, "sub:11_2", "user_id", CMD_DELETE, CMD_DELETE, "false");
    // checkAddToSkip(schema, "sub:12", "user_id", CMD_UPDATE_FIELD, CMD_UPDATE_FIELD, "true");
    // checkAddToSkip(schema, "sub:12_2", "user_id", CMD_UPDATE_FIELD, CMD_UPDATE_FIELD, "false")
        ;
    checkAddToSkip(schema, "sub:13", "price", CMD_ADD, CMD_ADD, "true");
    checkAddToSkip(schema, "sub:14", "price", CMD_ADD, CMD_SKIP, "true", "price");
    checkAddToSkip(schema, "sub:14_2", "price", CMD_ADD, CMD_SKIP, "false", "price");

}


TEST_F(ModifiedFieldsDocumentProcessorTest, testAddToSkip) {
    doTestAddToSkip(_schemaPtr);
    doTestAddToSkip(_schemaWithSubSchema);
}

void ModifiedFieldsDocumentProcessorTest::checkSimpleProcess(const char* msg,
        string kvMapStr, string modifyFields, string expectFieldsStr)
{
    KeyValueMap kvMap;
    kvMap["derivative_relationship"] = kvMapStr;

    RawDocumentPtr rawDocPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDocPtr->setField(HA_RESERVED_MODIFY_FIELDS, modifyFields);
    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDocPtr);
    extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);

    ModifiedFieldsDocumentProcessor processor;
    DocProcessorInitParam param;
    param.parameters = kvMap;
    param.schemaPtr = _schemaPtr;
    ASSERT_TRUE(processor.init(param)) << msg;
    ASSERT_TRUE(processor.process(extendDoc)) << msg;
    const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &modifiedFieldSet =
        extendDoc->getIndexExtendDoc()->getModifiedFieldSet();
    StringTokenizer st(expectFieldsStr, ",",
            StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ(st.getNumTokens(), modifiedFieldSet.size()) << msg;
    for (StringTokenizer::Iterator it = st.begin();
         it != st.end(); ++it)
    {
        fieldid_t fieldId = _schemaPtr->GetFieldSchema()->GetFieldId(*it);
        ASSERT_TRUE(modifiedFieldSet.find(fieldId) != modifiedFieldSet.end()) << msg;
    }
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testSimpleProcess) {
    checkSimpleProcess("sub:1", "", "f1;f2", "f1,f2");
    checkSimpleProcess("sub:2", "f2:f2a", "f1;f2", "f1,f2,f2a");
    checkSimpleProcess("sub:3", "f1:f1a;f2:f2a,f2b;", "f1;f2;f3", "f1,f2,f1a,f2a,f2b,f3");
    checkSimpleProcess("sub:4", "f1:f1a;f2:f2a,f2b", "", "");
    checkSimpleProcess("sub:5", "f1:f1a;f2:f2a,f2b", "f3", "f3");
    checkSimpleProcess("sub:6", "f1:f1a;f2:f2a,f2b", " ; ;;", "");
    checkSimpleProcess("sub:7", "fA:fB; fB:fC,fD; fD:fE", "fA", "fA,fB,fC,fD,fE");
    checkSimpleProcess("sub:8", "fB:fC,fD; fA:fB; fD:fE", "fA", "fA,fB");
    checkSimpleProcess("sub:9", "fA:fB; fB:fC,fD; fD:fE", "fB", "fB,fC,fD,fE");
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testInitRelationship) {
    const IndexPartitionSchemaPtr &schema = _schemaPtr;
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    KeyValueMap kvMap;
    kvMap["derivative_relationship"] = "f1:f1a,f1b;f2:f2a";

    {
        unique_ptr<ModifiedFieldsDocumentProcessor> processor(
                new ModifiedFieldsDocumentProcessor);
        DocProcessorInitParam param;
        param.parameters = kvMap;
        param.schemaPtr = schema;
        ASSERT_TRUE(processor->init(param));
        ASSERT_EQ((size_t)3, processor->_ModifiedFieldsModifierVec.size());
        ASSERT_EQ(fieldSchema->GetFieldId("f1"), 
                  processor->_ModifiedFieldsModifierVec[0]->GetSrcFieldId());
        ASSERT_EQ(fieldSchema->GetFieldId("f1a"), 
                  processor->_ModifiedFieldsModifierVec[0]->GetDstFieldId());
        ASSERT_EQ(fieldSchema->GetFieldId("f1"), 
                  processor->_ModifiedFieldsModifierVec[1]->GetSrcFieldId());
        ASSERT_EQ(fieldSchema->GetFieldId("f1b"), 
                  processor->_ModifiedFieldsModifierVec[1]->GetDstFieldId());
        ASSERT_EQ(fieldSchema->GetFieldId("f2"), 
                  processor->_ModifiedFieldsModifierVec[2]->GetSrcFieldId());
        ASSERT_EQ(fieldSchema->GetFieldId("f2a"), 
                  processor->_ModifiedFieldsModifierVec[2]->GetDstFieldId());
    }

    DocProcessorInitParam param;
    param.schemaPtr = schema;
    {
        unique_ptr<ModifiedFieldsDocumentProcessor> processor(
                new ModifiedFieldsDocumentProcessor);
        kvMap["derivative_relationship"] = ":f1a";
        param.parameters = kvMap;
        ASSERT_FALSE(processor->init(param));

        kvMap["derivative_relationship"] = "f1:";
        param.parameters = kvMap;
        ASSERT_FALSE(processor->init(param));
    }

    {
        unique_ptr<ModifiedFieldsDocumentProcessor> processor(
                new ModifiedFieldsDocumentProcessor);
        kvMap["derivative_relationship"] = ";";
        param.parameters = kvMap;
        ASSERT_TRUE(processor->init(param));
        ASSERT_TRUE(processor->_ModifiedFieldsModifierVec.empty());
    }
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessType) {
     ModifiedFieldsDocumentProcessorPtr processor(new ModifiedFieldsDocumentProcessor);
    DocumentProcessorPtr clonedProcessor(processor->clone());
    EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
    EXPECT_TRUE(!clonedProcessor->needProcess(DELETE_DOC));
    EXPECT_TRUE(!clonedProcessor->needProcess(UPDATE_FIELD));
    EXPECT_TRUE(!clonedProcessor->needProcess(DELETE_SUB_DOC));
    EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));
}

// docString: "m1,m2,s1,s2,s3,...;s1,s2^^s3"
ExtendDocumentPtr ModifiedFieldsDocumentProcessorTest::createDocument(
        const std::string &docString)
{
    StringTokenizer st(docString, ";", StringTokenizer::TOKEN_TRIM);
    assert(st.getNumTokens() == 1 || st.getNumTokens() == 2);

    string mainModifiedFieldString = st[0];
    StringUtil::replace(mainModifiedFieldString, ',', ';');
    RawDocumentPtr rawDocPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDocPtr->setField(HA_RESERVED_MODIFY_FIELDS, mainModifiedFieldString);
    ExtendDocumentPtr extendDoc(new ExtendDocument);
    extendDoc->setRawDocument(rawDocPtr);
    extendDoc->getRawDocument()->setDocOperateType(ADD_DOC);

    if (st.getNumTokens() == 2)
    {
        string subModifiedFieldString = st[1];
        StringUtil::replace(subModifiedFieldString, ',', ';');
        rawDocPtr->setField(HA_RESERVED_SUB_MODIFY_FIELDS, subModifiedFieldString);
        StringTokenizer subDocs(st[1], "^", StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < subDocs.getNumTokens(); ++i) {
            RawDocumentPtr subRawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
            subRawDoc->setField(HA_RESERVED_MODIFY_FIELDS, subDocs[i]);
            ExtendDocumentPtr subExtendDoc(new ExtendDocument);
            subExtendDoc->setRawDocument(subRawDoc);
            subExtendDoc->getRawDocument()->setDocOperateType(ADD_DOC);
            extendDoc->addSubDocument(subExtendDoc);
        }
    }

    return extendDoc;
}

// expectModifiedFields : "m1,m2...;s1,s2,s3,..;s1,s2^^s3"
bool ModifiedFieldsDocumentProcessorTest::checkResult(
        const IndexPartitionSchemaPtr &schema,
        const ExtendDocumentPtr &doc,
        const std::string &expectModifiedFields,
        bool expectSkip)
{
    StringTokenizer st(expectModifiedFields, ";", StringTokenizer::TOKEN_TRIM);
    assert(st.getNumTokens() == 2 || st.getNumTokens() ==3);
    
    // check main
    const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &mainSet =
        doc->getIndexExtendDoc()->getModifiedFieldSet();
    if (!checkModifiedFields(schema, mainSet, st[0])) {
        BS_LOG(ERROR, "check ModifiedFields in MAIN failed.");
        return false;
    }

    // check sub
    IndexPartitionSchemaPtr subSchema  = schema->GetSubIndexPartitionSchema();
    const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &subSet =
        doc->getIndexExtendDoc()->getSubModifiedFieldSet();
    if (!checkModifiedFields(subSchema, subSet, st[1])) {
        BS_LOG(ERROR, "check SubModifiedFields in MAIN failed.");
        return false;
    }

    // check sub docs
    string subString = (st.getNumTokens() == 2) ? "" : st[2];
    StringTokenizer subSt(subString, "^", StringTokenizer::TOKEN_TRIM);
    size_t subDocCount = (st.getNumTokens() == 2) ? 0 :subSt.getNumTokens();    
    if (subDocCount != doc->getSubDocumentsCount()) {
        BS_LOG(ERROR, "sub doc count [%lu], but expect sub doc count [%lu]",
               doc->getSubDocumentsCount(), subDocCount);
        return false;
    }
    for (size_t i = 0; i < doc->getSubDocumentsCount(); ++i) {
        ExtendDocumentPtr &subDoc = doc->getSubDocument(i);
        const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &subSet =
            subDoc->getIndexExtendDoc()->getModifiedFieldSet();
        if (!checkModifiedFields(subSchema, subSet, subSt[i])) {
            BS_LOG(ERROR, "check ModifiedFields in SUB[%lu] failed.", i);
            return false;
        }
    }
    // check skip
    DocOperateType docType = expectSkip ? SKIP_DOC : ADD_DOC;
    RawDocumentPtr rawDoc = doc->getRawDocument();
    if (rawDoc->getDocOperateType() != docType) {
        BS_LOG(ERROR, "check skip doc failed, actual type [%d]",
               rawDoc->getDocOperateType());
        return false;
    }
    return true;
}

bool ModifiedFieldsDocumentProcessorTest::checkModifiedFields(
        const IndexPartitionSchemaPtr &schema,
        const IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet &modifiedFieldSet,
        const std::string &expectModifiedFields)
{
    StringTokenizer st(expectModifiedFields, ",",
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() != modifiedFieldSet.size()) {
        BS_LOG(ERROR, "ModifiedFieldSet count not equal, "
               "actual [%lu], expect [%lu]",
               modifiedFieldSet.size(), st.getNumTokens());
        return false;
    }
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        fieldid_t fieldId = schema->GetFieldSchema()->GetFieldId(*it);
        if (modifiedFieldSet.find(fieldId) == modifiedFieldSet.end()) {
            BS_LOG(ERROR, "ModifiedFieldSet lack of field [%s]", it->c_str());
            return false;
        }
    }
    return true;
}

bool ModifiedFieldsDocumentProcessorTest::checkProcess(
        const std::string &relationRulesString,
        const std::string &ignoreRulesString,
        const std::string &docString,
        const std::string &expectModifiedFields,
        bool expectSkip)
{
    IndexPartitionSchemaPtr schema = _schemaPtr;
    KeyValueMap kvMap;
    kvMap["derivative_relationship"] = relationRulesString;
    kvMap["ignore_fields"] = ignoreRulesString;
    DocProcessorInitParam param;
    param.parameters = kvMap;
    param.schemaPtr = schema;

    ModifiedFieldsDocumentProcessor processor;
    if (!processor.init(param)) {
        BS_LOG(ERROR, "processor init failed.");
        return false;
    }

    ExtendDocumentPtr doc = createDocument(docString);
    if (!doc) {
        BS_LOG(ERROR, "create document failed.");
        return false;
    }

    if (!processor.process(doc)) {
        BS_LOG(ERROR, "processor process failed.");
        return false;
    }

    return checkResult(schema, doc, expectModifiedFields, expectSkip);
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessInitFail) {
    ASSERT_FALSE(checkProcess("f1:unknown", "", "", ";"));
    ASSERT_FALSE(checkProcess("", "unknown", "", ";"));
}

// relationRulesString  : "src1:dst1,dst2,...;src2:dst3;..."
// ignoreRulesString    : "ignore1,ignore2;..."
// docString            : "m1,m2,s1,s2,s3,...;s1,s2^^s3"
// expectModifiedFields : "m1,m2...;s1,s2,s3,..;s1,s2^^s3" 
void ModifiedFieldsDocumentProcessorTest::checkMainOnly()
{
    // no rule
    ASSERT_TRUE(checkProcess("", "", "", ";", false));
    ASSERT_TRUE(checkProcess("", "", "unknown", ";", true));
    ASSERT_TRUE(checkProcess("", "", "f1,f2", "f1,f2;", false));
    // ignore
    ASSERT_TRUE(checkProcess("", "f1", "f1", ";", true));
    ASSERT_TRUE(checkProcess("", "f1", "f2", "f2;", false));
    ASSERT_TRUE(checkProcess("", "f1", "unknown,f1,f2", "f2;", false));
    ASSERT_TRUE(checkProcess("", "f1,f2", "f1,f2", ";", true));
    ASSERT_TRUE(checkProcess("", "f1,f2", "unknown,f1,f2", ";", true));

    // relation
    ASSERT_TRUE(checkProcess("f1:f2", "", "f1", "f1,f2;", false));
    ASSERT_TRUE(checkProcess("f1:f2,f3", "", "f1", "f1,f2,f3;", false));
    ASSERT_TRUE(checkProcess("f1:f2;f2:f3", "", "f1", "f1,f2,f3;", false));
    ASSERT_TRUE(checkProcess("unknown:f2", "", "unknown", "f2;", false));
    ASSERT_TRUE(checkProcess("unknown:f2;f2:f3", "", "unknown", "f2,f3;", false));
    // mix
    ASSERT_TRUE(checkProcess("f1:f2,f3", "f1", "f1", "f2,f3;", false));
    ASSERT_TRUE(checkProcess("f1:f2,f3;unknown:fA", "f1,f3",
                             "f1,unknown", "f2,fA;", false));
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessWithoutSub) {
    checkMainOnly();
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessWithSub) {
    _schemaPtr = _schemaWithSubSchema;

    checkMainOnly();

    ASSERT_TRUE(checkProcess("", "", "s1", ";", false)) << "no rule";
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessWithSubForIgnore) {
    _schemaPtr = _schemaWithSubSchema;

    ASSERT_TRUE(checkProcess("", "f1", "f1", ";", true)) 
        << "Main, no sub doc";
    ASSERT_TRUE(checkProcess("", "s1", "s1", ";", false))
        << "Sub, no sub doc";
    ASSERT_TRUE(checkProcess("", "f1,s1", "f1,s1", ";", false))
        << "Main and Sub, no sub doc";

    ASSERT_TRUE(checkProcess("", "", ";", ";;", false))
        << "Add, has one sub doc";
    ASSERT_TRUE(checkProcess("", "f1", "f1;", ";;", true))
        << "Main, has one sub doc";
    ASSERT_TRUE(checkProcess("", "f1", "f1,s1;s1", ";s1;s1", false))
        << "Main, has one sub doc";
    ASSERT_TRUE(checkProcess("", "s1", "s1;s1", ";;", true))
        << "Sub, has one sub doc";
    ASSERT_TRUE(checkProcess("", "f1,s1", "f1,s1;s1", ";;", true))
        << "Main and Sub, has one sub doc";
    ASSERT_TRUE(checkProcess("", "f1,s1", "f1,s1;", ";;", false))
        << "Main and Sub, has one sub doc, not set HA_RESERVED_SUB_MODIFY_FIELDS";

    ASSERT_TRUE(checkProcess("", "f1,s1", "f1,s1;s1^s1", ";;^", true))
        << "Main and Sub, has two sub doc";
    ASSERT_TRUE(checkProcess("", "f1,s1", "f1,s1,s2;s1^s2", ";s2;^s2", false))
        << "Main and Sub, has two sub doc";
}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessWithSubForRelation) {
    _schemaPtr = _schemaWithSubSchema;

    // relation: has no sub doc
    ASSERT_TRUE(checkProcess("", "", "f1,s1", ";", false))
        << "no rule";
    ASSERT_TRUE(checkProcess("f1:f2", "", "f1", "f1,f2;", false))
        << "Main to Main";
    ASSERT_TRUE(checkProcess("f1:s1", "", "f1", "f1;s1", false))
        << "Main to Sub";
    ASSERT_TRUE(checkProcess("s1:s2", "", "s1", ";", false))
        << "Sub to Sub";
    ASSERT_TRUE(checkProcess("s1:f1", "", "s1", ";", false))
        << "Sub to Main";
    ASSERT_TRUE(checkProcess("unknown:f2", "", "unknown", "f2;", false))
        << "Unknown to Main";
    ASSERT_TRUE(checkProcess("unknown:s1", "", "unknown", ";s1", false))
        << "Unknown to Sub";

    // relation: has on sub doc
    ASSERT_TRUE(checkProcess("", "", "f1,s1;s1", "f1;s1;s1", false))
        << "no rule, has one sub doc";
    ASSERT_TRUE(checkProcess("f1:f2", "", "f1;", "f1,f2;;", false))
        << "Main to Main, has one sub doc";
    ASSERT_TRUE(checkProcess("s1:f1", "", "s1;s1", "f1;s1;s1", false))
        << "Sub to Main, has one sub doc";
    ASSERT_TRUE(checkProcess("s1:s2", "", "s1;s1", ";s1,s2;s1,s2", false))
        << "Sub to Sub, has one sub doc";
    ASSERT_TRUE(checkProcess("f1:s1", "", "f1;", "f1;s1;s1", false))
        << "Main to Sub, has one sub doc";
    ASSERT_TRUE(checkProcess("unknown:f2", "", "unknown;", "f2;;", false))
        << "Unknown to Main, has one sub doc";
    ASSERT_TRUE(checkProcess("unknown:s1", "", "unknown;", ";s1;s1", false))
        << "Unknown to Sub, has one sub doc";


    // relation: has on two doc
    ASSERT_TRUE(checkProcess("", "", "f1,s1;s1^", "f1;s1;s1^", false))
        << "no rule, has two sub doc";
    ASSERT_TRUE(checkProcess("f1:f2", "", "f1;^", "f1,f2;;^", false))
        << "Main to Main, has two sub doc";
    ASSERT_TRUE(checkProcess("s1:f1", "", "s1;s1^", "f1;s1;s1^", false))
        << "Sub to Main, has two sub doc";
    ASSERT_TRUE(checkProcess("s1:s2", "", "s1;s1^", ";s1,s2;s1,s2^", false))
        << "Sub to Sub, has two sub doc";
    ASSERT_TRUE(checkProcess("f1:s1", "", "f1;^", "f1;s1;s1^s1", false))
        << "Main to Sub, has two sub doc";
    ASSERT_TRUE(checkProcess("unknown:f2", "", "unknown;^", "f2;;^", false))
        << "Unknown to Main, has two sub doc";
    ASSERT_TRUE(checkProcess("unknown:s1", "", "unknown;^", ";s1;s1^s1", false))
        << "Unknown to Sub, has two sub doc";

}

TEST_F(ModifiedFieldsDocumentProcessorTest, testProcessWithSubFail) {
    _schemaPtr = _schemaWithSubSchema;

    ASSERT_TRUE(checkProcess("", "", "s1;", ";;", false))
        << "sub field in main, but not in any sub docs";
    ASSERT_TRUE(checkProcess("", "", "f1;s1", ";;", false))
        << "sub field in a sub doc, but not in main";

}

}
}
