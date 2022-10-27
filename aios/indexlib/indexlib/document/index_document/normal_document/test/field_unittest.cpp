
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/key_hasher_typed.h"
#include <sstream>

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);

class FieldTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(FieldTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForCreateSection()
    {
        IndexTokenizeField* field = new IndexTokenizeField();
        Section* pSec = field->CreateSection();
        pSec->SetSectionId(0);
        pSec = field->CreateSection();
        pSec->SetSectionId(1);

        INDEXLIB_TEST_TRUE(2==field->GetSectionCount());

        field->Reset();

        INDEXLIB_TEST_TRUE(0 == field->GetSectionCount());

        INDEXLIB_TEST_TRUE(NULL == field->GetSection(0));
        INDEXLIB_TEST_TRUE(NULL == field->GetSection(1));

        pSec = field->CreateSection();
        INDEXLIB_TEST_TRUE(0 == pSec->GetSectionId());
        pSec = field->CreateSection();
        INDEXLIB_TEST_TRUE(1 == pSec->GetSectionId());
        INDEXLIB_TEST_TRUE(2 == field->GetSectionCount());
       
        delete field;
        
    }

    void TestCaseForAddSection()
    {
        const size_t sectionNum = 3;
        vector<Section*> sectionVec;
        CreateSections(sectionNum, sectionVec);
        IndexTokenizeFieldPtr field(new IndexTokenizeField());
        for (size_t i = 0; i < sectionNum; ++i)
        {
            field->AddSection(sectionVec[i]);
        }
        INDEXLIB_TEST_EQUAL(sectionNum, field->GetSectionCount());
        for (size_t i = 0; i < sectionNum; ++i)
        {
            Section* section = field->GetSection((sectionid_t)i);
            INDEXLIB_TEST_TRUE(section != NULL);
            INDEXLIB_TEST_EQUAL(section->GetSectionId(), (sectionid_t)i);
        }
    }

    void TestCaseForOperatorEqual()
    {
        vector<IndexTokenizeField*> fields1;
        vector<IndexTokenizeField*> fields2;
        CreateFields(10, fields1);
        CreateFields(10, fields2);
        for(uint32_t i = 0; i < 10; ++i)
        {
            INDEXLIB_TEST_TRUE(*fields1[i] == *fields2[i]);
            for (uint32_t j = 0; j < 10; ++j)
            {
                if (i == j)
                {
                    INDEXLIB_TEST_TRUE(*fields1[i] == *fields1[j]);
                }
                else
                {
                    INDEXLIB_TEST_TRUE(*fields1[i] != *fields1[j]);
                }
            }
        }

        for (uint32_t i = 0; i < 10; ++i)
        {
            delete fields1[i];
            delete fields2[i];
        }
    }

    void TestCaseForSectionNumberOutOfRange()
    {
        IndexTokenizeField field;
        for (size_t i = 0; i < Field::MAX_SECTION_PER_FIELD; ++i)
        {
            Section* section = field.CreateSection();
            INDEXLIB_TEST_TRUE(section != NULL);
        }

        Section* section = field.CreateSection();
        INDEXLIB_TEST_TRUE(section == NULL);
    }

    void TestCaseSerializeField()
    {
        vector<IndexTokenizeField*> fields1;
        vector<IndexTokenizeField*> fields2;
        CreateFields(10, fields1);
        autil::DataBuffer dataBuffer;

        dataBuffer.write(fields1);
        dataBuffer.read(fields2);

        for(uint32_t i = 0; i < 10; ++i)
        {
            INDEXLIB_TEST_TRUE(*fields1[i] == *fields2[i]);
        }
        for (uint32_t i = 0; i < 10; ++i)
        {
            delete fields1[i];
            delete fields2[i];
        }
    }


private:

    void CreateSections(uint32_t sectionNum, vector<Section*>& sections)
    {
        vector<Token*> tokens;
        for (uint32_t i = 0; i < sectionNum; ++i)
        {
            Section* section = new Section();
            stringstream ss;
            ss << "token" << i;
            DefaultHasher hasher;
            uint64_t hashKey;
            hasher.GetHashKey(ss.str().c_str(), hashKey);
            section->CreateToken(hashKey, (pos_t)i, (pospayload_t)i);
            sections.push_back(section);
        }
    }

    void CreateFields(uint32_t fieldCount, vector<IndexTokenizeField*>& fields)
    {
        fields.reserve(fieldCount);
        for (uint32_t i = 0; i < fieldCount; ++i)
        {
            vector<Section*> sectionVec;
            uint32_t sectionNum = i + 1;
            CreateSections(sectionNum, sectionVec);
            IndexTokenizeField* field = new IndexTokenizeField();
            field->SetFieldId(i);
            for (size_t i = 0; i < sectionNum; ++i)
            {
                field->AddSection(sectionVec[i]);
            }
            fields.push_back(field);
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(FieldTest, TestCaseForCreateSection);
INDEXLIB_UNIT_TEST_CASE(FieldTest, TestCaseForAddSection);
INDEXLIB_UNIT_TEST_CASE(FieldTest, TestCaseForOperatorEqual);
INDEXLIB_UNIT_TEST_CASE(FieldTest, TestCaseForSectionNumberOutOfRange);
INDEXLIB_UNIT_TEST_CASE(FieldTest, TestCaseSerializeField);

IE_NAMESPACE_END(document);
