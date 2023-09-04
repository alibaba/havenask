#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/section.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/KeyHasherTyped.h"
using namespace std;
#define NUM_SECTIONS 100

using namespace indexlib::util;
namespace indexlib { namespace document {

class SectionTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SectionTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForCreateTokenOverFlow()
    {
        Section* section = new Section(10000);
        string s1 = "hello";
        Token* pToken = NULL;
        uint32_t maxCount = Section::MAX_TOKEN_PER_SECTION;
        std::set<size_t> extendPoints;
        extendPoints.insert(10000);
        extendPoints.insert(20000);
        extendPoints.insert(40000);
        for (uint32_t i = 0; i < maxCount; ++i) {
            DefaultHasher hasher;
            uint64_t hashKey;
            hasher.GetHashKey(s1.c_str(), s1.length(), hashKey);
            pToken = section->CreateToken(hashKey, 0, 0);
            INDEXLIB_TEST_TRUE(pToken);
            if (extendPoints.find(i) != extendPoints.end()) {
                uint32_t newCapacity = std::min(i * 2, Section::MAX_TOKEN_PER_SECTION);
                INDEXLIB_TEST_EQUAL(newCapacity, section->GetCapacity());
            }
        }
        INDEXLIB_TEST_TRUE(maxCount == section->GetTokenCount());
        DefaultHasher hasher;
        uint64_t hashKey;
        hasher.GetHashKey(s1.c_str(), s1.length(), hashKey);
        pToken = section->CreateToken(hashKey, 0, 0);
        INDEXLIB_TEST_TRUE(NULL == pToken);
        INDEXLIB_TEST_TRUE(maxCount == section->GetTokenCount());
        INDEXLIB_TEST_EQUAL(maxCount, section->GetCapacity());
        delete section;
    }

    void TestCaseForCreateToken()
    {
        Section* section = new Section(50);
        string s1 = "hello";
        DefaultHasher hasher;
        uint64_t hashKey;
        hasher.GetHashKey(s1.c_str(), s1.length(), hashKey);
        Token* pToken = section->CreateToken(hashKey, 0, 0);
        string s2 = "holk";
        hasher.GetHashKey(s2.c_str(), s2.length(), hashKey);
        pToken = section->CreateToken(hashKey, 0, 0);
        (void)pToken;
        INDEXLIB_TEST_TRUE(2 == section->GetTokenCount());

        section->Reset();

        INDEXLIB_TEST_TRUE(0 == section->GetTokenCount());
        // INDEXLIB_TEST_TRUE(NULL == section->GetToken(0));
        // INDEXLIB_TEST_TRUE(NULL == section->GetToken(1));

        for (size_t i = 0; i < Section::MAX_TOKEN_PER_SECTION; i++) {
            stringstream ss;
            ss << "Token" << i;
            DefaultHasher hasher;
            uint64_t hashKey;
            const string& str = ss.str();
            hasher.GetHashKey(str.c_str(), str.length(), hashKey);
            section->CreateToken(hashKey, 0, 0);
        }

        INDEXLIB_TEST_TRUE(Section::MAX_TOKEN_PER_SECTION == section->GetTokenCount());
        delete section;
    }

    void TestCaseForEqual()
    {
        vector<Section*> sections1;
        vector<Section*> sections2;
        GenerateSections(NUM_SECTIONS, sections1);
        GenerateSections(NUM_SECTIONS, sections2);
        for (size_t i = 0; i < sections1.size(); ++i) {
            for (size_t j = i; j < sections2.size(); ++j) {
                if (i == j)
                    INDEXLIB_TEST_TRUE(*(sections1[i]) == *(sections2[j]));
                else
                    INDEXLIB_TEST_TRUE(*(sections1[i]) != *(sections2[j]));
            }
        }

        for (uint32_t i = 0; i < sections1.size(); ++i) {
            delete sections1[i];
        }
        for (uint32_t i = 0; i < sections2.size(); ++i) {
            delete sections2[i];
        }
    }

    void TestCaseForConstructOverFlow()
    {
        Section* section = new Section(65536);
        Token* pToken = section->CreateToken(0, 0, 0);
        INDEXLIB_TEST_TRUE(pToken);
        Token* pToken2 = section->CreateToken(0, 0, 0);
        INDEXLIB_TEST_TRUE(pToken2);
        delete section;
    }

    void TestCaseSerializeSection()
    {
        vector<Section*> sections1;
        vector<Section*> sections2;
        GenerateSections(NUM_SECTIONS, sections1);
        autil::DataBuffer dataBuffer;

        dataBuffer.write(sections1);
        dataBuffer.read(sections2);

        for (size_t i = 0; i < sections1.size(); ++i) {
            for (size_t j = i; j < sections2.size(); ++j) {
                if (i == j)
                    INDEXLIB_TEST_TRUE(*(sections1[i]) == *(sections2[j]));
                else
                    INDEXLIB_TEST_TRUE(*(sections1[i]) != *(sections2[j]));
            }
        }

        for (uint32_t i = 0; i < sections1.size(); ++i) {
            delete sections1[i];
        }
        for (uint32_t i = 0; i < sections2.size(); ++i) {
            delete sections2[i];
        }
    }

private:
    void GenerateSections(int num, vector<Section*>& sections)
    {
        for (int i = 0; i < num; ++i) {
            Section* ptr = new Section;
            ptr->SetSectionId(i % 65536);
            ptr->SetWeight((i + 1) % 65536);
            ptr->SetLength((i + 2) % 65536);
            for (int k = 0; k < i % 10 + 1; k++) {
                stringstream ss;
                ss << k;
                DefaultHasher hasher;
                uint64_t hashKey;
                const string& str = ss.str();
                hasher.GetHashKey(str.c_str(), str.length(), hashKey);
                ptr->CreateToken(hashKey, (pos_t)k, (pospayload_t)(k % 256));
            }

            sections.push_back(ptr);
        }
    }

private:
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(SectionTest, TestCaseForEqual);
INDEXLIB_UNIT_TEST_CASE(SectionTest, TestCaseForCreateToken);
INDEXLIB_UNIT_TEST_CASE(SectionTest, TestCaseForCreateTokenOverFlow);
INDEXLIB_UNIT_TEST_CASE(SectionTest, TestCaseForConstructOverFlow);
INDEXLIB_UNIT_TEST_CASE(SectionTest, TestCaseSerializeSection);
}} // namespace indexlib::document
