#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);

IE_NAMESPACE_BEGIN(document);

class NormalDocumentTest : public INDEXLIB_TESTBASE
{
public:
    NormalDocumentTest() {}
    ~NormalDocumentTest() {}

    DECLARE_CLASS_NAME(NormalDocumentTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForOperatorEqual();
    void TestCaseSerializeDocument();
    void TestCaseSerializeDocumentWithIndexRawFields();
    void TestCaseForDeserializeLegacyDocument();
    void TestCaseForCompatibleHighVersion();
    void TestAddTag();

private:
    static void CreateDocument(std::vector<NormalDocumentPtr>& docs);
    static NormalDocumentPtr CreateDocument(uint32_t idx);
    static void CreateFields(const std::vector<fieldid_t>& fieldIds,
                             std::vector<IndexTokenizeField*>& fieldVec,
                             autil::mem_pool::Pool* pool);

    void SerializeInLegacyFormat(uint32_t docVersionId, NormalDocument& doc,
                                 autil::DataBuffer& dataBuffer, bool needSetDocVersion = true);

    void CheckSerializeLegacyVersion(uint32_t version, const NormalDocumentPtr& doc);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestCaseForOperatorEqual);
INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestCaseSerializeDocument);
INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestCaseSerializeDocumentWithIndexRawFields);
INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestCaseForDeserializeLegacyDocument);
INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestCaseForCompatibleHighVersion);
INDEXLIB_UNIT_TEST_CASE(NormalDocumentTest, TestAddTag);

IE_NAMESPACE_END(document);
