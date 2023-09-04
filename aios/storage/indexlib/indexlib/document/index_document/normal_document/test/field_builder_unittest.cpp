#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/field_builder.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace indexlib::config;

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::util;
namespace indexlib { namespace document {

class FieldBuilderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(FieldBuilderTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForAddToken()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 3;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321, N, 123, N, 4, N, N, N; 22, N";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenOverFlow()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = Section::MAX_SECTION_LENGTH;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(Section::MAX_TOKEN_PER_SECTION);

        string str = "321";
        for (size_t i = 0; i < Section::MAX_TOKEN_PER_SECTION + 1; ++i) {
            str += ", 1";
        }

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, maxSectionLen);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenWithPosition()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 3;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321, N, 123, N, 4, N, N, N, N, N, def; 22, N, abc";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildFieldWithPosition(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForEmptySection()
    {
        {
            IndexTokenizeField field;
            uint32_t maxSectionLen = 3;
            autil::mem_pool::Pool pool;
            FieldBuilder fieldBuilder(&field, &pool);
            fieldBuilder.SetMaxSectionLen(maxSectionLen);

            fieldBuilder.BeginSection(1);
            fieldBuilder.EndSection();
            fieldBuilder.EndField();
            INDEXLIB_TEST_EQUAL((size_t)0, field.GetSectionCount());
        }

        {
            IndexTokenizeField field;
            uint32_t maxSectionLen = 3;
            autil::mem_pool::Pool pool;
            FieldBuilder fieldBuilder(&field, &pool);
            fieldBuilder.SetMaxSectionLen(maxSectionLen);

            fieldBuilder.EndSection();
            fieldBuilder.EndField();
            INDEXLIB_TEST_EQUAL((size_t)0, field.GetSectionCount());
        }

        {
            IndexTokenizeField field;
            uint32_t maxSectionLen = 3;
            autil::mem_pool::Pool pool;
            FieldBuilder fieldBuilder(&field, &pool);
            fieldBuilder.SetMaxSectionLen(maxSectionLen);

            fieldBuilder.BeginSection(1);
            fieldBuilder.EndField();
            INDEXLIB_TEST_EQUAL((size_t)0, field.GetSectionCount());
        }
    }

    void TestCaseForAddTokenWithEmptySection()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 5;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321, N, 123, N, 4, N, N, N; 22; 23, 13, N, 111";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenWithEmptySectionAtEnd()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 5;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321, N, 123, N, 4, N, N, N; 23, 13, N, 111;22";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenWithAllPlaceHolder()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 5;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321, N, N, N, N, N, N, N; 22; 23, N, N, N";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenWithAllEmptySection()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 5;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        string str = "321; 22; 23";

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        CheckField(answerVec, field);
    }

    void TestCaseForAddTokenWithMaxSectionNum()
    {
        IndexTokenizeField field;
        uint32_t maxSectionLen = 3;
        autil::mem_pool::Pool pool;
        FieldBuilder fieldBuilder(&field, &pool);
        fieldBuilder.SetMaxSectionLen(maxSectionLen);

        uint32_t i;
        string str = "";
        size_t maxSectionPerField = Field::MAX_SECTION_PER_FIELD;
        for (i = 0; i < maxSectionPerField / 2; ++i) {
            str += (i % 2 == 1) ? "4, N, a, N;" : "2, a, N, a;";
        }

        SectionVector sectionVec;
        MakeFieldString(str, sectionVec, 10000);
        BuildField(fieldBuilder, sectionVec, false);

        SectionVector answerVec;
        MakeFieldString(str, answerVec, maxSectionLen);

        INDEXLIB_TEST_EQUAL(maxSectionPerField, field.GetSectionCount());
        CheckField(answerVec, field);

        fieldBuilder.BeginSection(1);
        DefaultHasher hasher;
        uint64_t hashKey;
        hasher.GetHashKey("a", 1, hashKey);
        bool ret = fieldBuilder.AddToken(hashKey, 1);
        INDEXLIB_TEST_TRUE(!ret);
        ret = fieldBuilder.AddPlaceHolder();
        INDEXLIB_TEST_TRUE(!ret);
        fieldBuilder.EndSection();
        fieldBuilder.EndField();

        INDEXLIB_TEST_EQUAL(maxSectionPerField, field.GetSectionCount());
        CheckField(answerVec, field);
    }

private:
    typedef std::vector<std::string> TokenVector;
    typedef pair<section_weight_t, std::vector<std::string>> FakeSection;
    typedef std::vector<FakeSection> SectionVector;

    void MakeFieldString(const string& str, SectionVector& sectionVec, uint32_t maxSectionLen)
    {
        // field string format: weight,tokenstr1,tokenstr2...;weight,tokenstr3...
        StringTokenizer st(str, ";", autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            TokenVector tokenVec;
            StringTokenizer st2(st[i], ",",
                                autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
            section_weight_t weight = (section_weight_t)autil::StringUtil::strToInt32WithDefault(st2[0].data(), 0);

            for (size_t j = 1; j < st2.getNumTokens(); ++j) {
                tokenVec.push_back(st2[j]);
                if (tokenVec.size() == maxSectionLen - 1) {
                    sectionVec.push_back(make_pair(weight, tokenVec));
                    tokenVec.clear();
                }
            }
            if (tokenVec.size() > 0) {
                sectionVec.push_back(make_pair(weight, tokenVec));
            }
        }
    }

    void BuildField(FieldBuilder& builder, const SectionVector& sectionVec, bool endField = true)
    {
        for (SectionVector::const_iterator it = sectionVec.begin(); it != sectionVec.end(); ++it) {
            section_weight_t weight = (*it).first;
            const vector<string>& tokenVec = (*it).second;

            builder.BeginSection(weight);
            for (TokenVector::const_iterator it2 = tokenVec.begin(); it2 != tokenVec.end(); ++it2) {
                if (*it2 == "N") {
                    INDEXLIB_TEST_TRUE(builder.AddPlaceHolder());
                } else {
                    DefaultHasher hasher;
                    uint64_t hashKey;
                    hasher.GetHashKey(it2->c_str(), it2->length(), hashKey);
                    INDEXLIB_TEST_TRUE(builder.AddToken(hashKey, weight % 5));
                }
            }
            builder.EndSection();
        }
        if (endField) {
            builder.EndField();
        }
    }

    void BuildFieldWithPosition(FieldBuilder& builder, const SectionVector& sectionVec, bool endField = true)
    {
        pos_t pos = 0;
        for (SectionVector::const_iterator it = sectionVec.begin(); it != sectionVec.end(); ++it) {
            section_weight_t weight = (*it).first;
            const vector<string>& tokenVec = (*it).second;

            builder.BeginSection(weight);
            for (TokenVector::const_iterator it2 = tokenVec.begin(); it2 != tokenVec.end(); ++it2) {
                if (*it2 == "N") {
                    pos++;
                } else {
                    DefaultHasher hasher;
                    uint64_t hashKey;
                    hasher.GetHashKey(it2->c_str(), it2->length(), hashKey);
                    builder.AddTokenWithPosition(hashKey, pos++, weight % 5);
                }
            }
            builder.EndSection();
        }
        if (endField) {
            builder.EndField();
        }
    }

    void CheckField(const SectionVector& sectionVec, const IndexTokenizeField& field)
    {
        INDEXLIB_TEST_EQUAL(sectionVec.size(), field.GetSectionCount());

        size_t totalTokens = 0;
        uint32_t sectionGap = 0;
        pos_t pos = 0;
        for (size_t i = 0; i < sectionVec.size(); ++i) {
            section_weight_t weight = sectionVec[i].first;
            const vector<string>& tokenVec = sectionVec[i].second;

            Section* section = field.GetSection((sectionid_t)i);
            INDEXLIB_TEST_TRUE(section != NULL);
            assert(section);
            INDEXLIB_TEST_EQUAL((section_len_t)tokenVec.size() + 1, section->GetLength());
            INDEXLIB_TEST_EQUAL(weight, section->GetWeight());
            INDEXLIB_TEST_EQUAL((sectionid_t)i, section->GetSectionId());

            size_t numTokens = 0;
            for (size_t j = 0; j < tokenVec.size(); ++j) {
                if (tokenVec[j] != "N") {
                    const Token* token = section->GetToken(numTokens++);
                    INDEXLIB_TEST_TRUE(token != NULL);
                    INDEXLIB_TEST_EQUAL((pos_t)(totalTokens + j + sectionGap) - pos, token->GetPosIncrement());
                    INDEXLIB_TEST_EQUAL(weight % 5, token->GetPosPayload());
                    pos = (pos_t)(totalTokens + j);
                    sectionGap = 0;
                }
            }
            totalTokens += tokenVec.size();
            sectionGap++;
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddToken);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenOverFlow);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForEmptySection);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithEmptySection);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithEmptySectionAtEnd);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithAllPlaceHolder);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithAllEmptySection);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithMaxSectionNum);
INDEXLIB_UNIT_TEST_CASE(FieldBuilderTest, TestCaseForAddTokenWithPosition);
}} // namespace indexlib::document
