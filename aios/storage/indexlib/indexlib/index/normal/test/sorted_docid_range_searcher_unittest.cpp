#include "indexlib/index/normal/test/sorted_docid_range_searcher_unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;

namespace indexlib { namespace index {

void SortedDocidRangeSearcherTest::CaseSetUp()
{
    mRoot = GET_TEMP_DATA_PATH();
    mTokenizeFlag = StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY;
    mBaseSegId = 0;
}

void SortedDocidRangeSearcherTest::CaseTearDown() {}

void SortedDocidRangeSearcherTest::FloatTypeWrapper(const string& segsStr, const string& sortTypesStr,
                                                    const string& attrNamesStr, const string& attrValuesStr,
                                                    const string& dimensStr, const string& rangeLimitStr,
                                                    const string& expectedRangesStr)
{
    InternalTest<float>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                        expectedRangesStr);
    InternalTest<double>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                         expectedRangesStr);
}

void SortedDocidRangeSearcherTest::IntTypeWrapper(const string& segsStr, const string& sortTypesStr,
                                                  const string& attrNamesStr, const string& attrValuesStr,
                                                  const string& dimensStr, const string& rangeLimitStr,
                                                  const string& expectedRangesStr)
{
    InternalTest<int8_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                         expectedRangesStr);
    InternalTest<int16_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<int32_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<int64_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
}

void SortedDocidRangeSearcherTest::UIntTypeWrapper(const string& segsStr, const string& sortTypesStr,
                                                   const string& attrNamesStr, const string& attrValuesStr,
                                                   const string& dimensStr, const string& rangeLimitStr,
                                                   const string& expectedRangesStr)
{
    InternalTest<uint8_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<uint16_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
    InternalTest<uint32_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
    InternalTest<uint64_t>(segsStr, sortTypesStr, attrNamesStr, attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeOneSegmentASC()
{
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5", "simple:-2~5", "0-10", "2-10");
    // test element not exist
    IntTypeWrapper("10", "+", "simple", "-4,-3,-1,-1,0,1,2,2,4,5", "simple:-2~3", "0-10", "2-8");
    // test out of range
    IntTypeWrapper("10", "+", "simple", "-4,-3,-1,-1,0,1,2,2,4,5", "simple:7~10", "0-10", "");
    // test null field
    IntTypeWrapper("13", "+", "simple", "N,N,N,-4,-3,-2,-1,0,1,2,3,4,5", "simple:-2~5", "0-13", "5-13");
    IntTypeWrapper("13", "+", "simple", "N,N,N,N,N,N,N,N,N,N,N,-4,-3", "simple:-3~5", "0-13", "12-13");
    IntTypeWrapper("13", "+", "simple", "N,N,N,N,N,N,N,N,-4,-3,-1,-1,0", "simple:2~5", "0-13", "");
    IntTypeWrapper("13", "+", "simple", "N,N,N,N,N,N,N,N,-4,-3,-1,-1,0", "simple:^~-1", "0-13", "8-12");
    IntTypeWrapper("13", "+", "simple", "N,N,N,N,N,N,MIN,MIN,-4,-3,-1,-1,0", "simple:^~^", "0-13", "6-13");

    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9", "simple:2~5", "0-10", "2-6");
    FloatTypeWrapper("8", "+", "simple", "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1", "simple:-2.1~2.1", "0-8", "1-7");

    IntTypeWrapper("6", "+", "simple", "-2,-1,-1,1,1,2", "simple:-1~1", "0-6", "1-5");
    UIntTypeWrapper("4", "+", "simple", "0,1,1,2", "simple:1~1", "0-4", "1-3");
    FloatTypeWrapper("5", "+", "simple", "-1.1,1.1,2.2,2.2,3.3", "simple:2.2", "0-5", "2-4");
}
void SortedDocidRangeSearcherTest::TestCaseForINFINITE()
{
    IntTypeWrapper("4", "+", "simple", "-2,-1,-1,2", "simple:^~^", "0-4", "0-4");
    FloatTypeWrapper("5", "+", "simple", "1.1,2.2,2.2,2.2,3.3", "simple:2.2~^", "0-5", "1-5");
    FloatTypeWrapper("5", "-", "simple", "2.2,1.1,-2.2,-2.2,-2.2", "simple:^~-2.2", "0-5", "0-5");
}

void SortedDocidRangeSearcherTest::TestCaseForRangeSwap()
{
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5", "simple:-2~5", "0-10", "2-10");
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5", "simple:5~-2", "0-10", "2-10");
    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9", "simple:2~5", "0-10", "2-6");
    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9", "simple:5~2", "0-10", "2-6");
    FloatTypeWrapper("8", "+", "simple", "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1", "simple:-2.1~2.1", "0-8", "1-7");
    FloatTypeWrapper("8", "+", "simple", "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1", "simple:2.1~-2.1", "0-8", "1-7");
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeOneSegmentDESC()
{
    IntTypeWrapper("4", "-", "simple", "2,-1,-1,-2", "simple:-1~-1", "0-4", "1-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:1~1", "0-4", "1-3");
    // test null field
    IntTypeWrapper("10", "-", "simple", "10,8,6,2,-1,-1,-2,N,N,N", "simple:-1~6", "0-10", "2-6");
    IntTypeWrapper("10", "-", "simple", "-1,-2,N,N,N,N,N,N,N,N", "simple:-1~1", "0-10", "0-1");
    IntTypeWrapper("10", "-", "simple", "4,-1,-2,-3,MIN,MIN,MIN,N,N,N,N,N", "simple:-1~^", "0-10", "1-7");
    IntTypeWrapper("10", "-", "simple", "-1,-2,-3,MIN,MIN,MIN,N,N,N,N,N", "simple:^~-2", "0-10", "0-2");

    FloatTypeWrapper("5", "-", "simple", "3.3,2.2,2.2,2.2,1.1", "simple:2.2", "0-5", "1-4");

    IntTypeWrapper("10", "-", "simple", "9,8,7,6,5,4,3,2,1,0", "simple:2~5", "0-10", "4-8");
    FloatTypeWrapper("8", "-", "simple", "3.1,2.1,1.1,0.1,-0.1,-1.1,-2.1,-3.1", "simple:-2.1~2.1", "0-8", "1-7");
    FloatTypeWrapper("8", "-", "simple", "3.1,2.1,1.1,0.1,-0.1,-1.1,-2.1,-3.1", "simple:-2.1~2.1,-3.1,4.1~3.1", "0-8",
                     "0-1,1-7,7-8");
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeMultiSegment()
{
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0", "simple:7~2", "0-10", "2-8");
    IntTypeWrapper("5,7", "-", "simple", "9,8,7,6,5;4,3,2,1,0,N,N", "simple:7~2", "0-12", "2-8");
    IntTypeWrapper("5,7", "-", "simple", "9,8,7,6,5;4,3,2,1,0,N,N", "simple:^~2", "0-12", "0-8");

    IntTypeWrapper("8,5", "+", "simple", "N,N,N,0,1,2,3,4;5,6,7,8,9", "simple:2~7", "0-13", "5-11");
    IntTypeWrapper("5,5", "+", "simple", "0,1,2,3,4;5,6,7,8,9", "simple:2~7", "0-10", "2-8");
    IntTypeWrapper("8,5", "+", "simple", "N,N,N,0,1,2,3,4;5,6,7,8,9", "simple:^~2", "0-13", "3-6");

    IntTypeWrapper("5,5,5", "+", "simple", "-4,-4,-2,-2,-1;0,1,2,3,4;5,7,7,8,8", "simple:-2~7", "0-15", "2-13");

    IntTypeWrapper("5,5,5", "+", "simple", "N,N,1,3,4;N,1,2,3,4;N,7,7,8,8", "simple:^~3", "0-15", "2-4");
    IntTypeWrapper("5,5,5", "+", "simple", "N,N,1,3,4;N,1,2,3,4;N,7,7,8,8", "simple:6~^", "0-15", "11-15");
    IntTypeWrapper("7,2", "-", "simple", "7,6,4,2,MIN,N,N;MIN,N", "simple:^~^", "0-15", "0-5");
}

void SortedDocidRangeSearcherTest::TestCaseForMultiAttributeOneSegment()
{
    IntTypeWrapper("5", "+,+", "simple,fat", "-4,-4,3,3,3|2,5,2,4,4", "simple:-4,3;fat:1~8", "0-5", "0-2,2-5");
    IntTypeWrapper("5", "+,+", "simple,fat", "-4,-4,3,3,3|2,5,2,4,4", "simple:-4,2,-1;fat:5", "0-5", "1-2");
    IntTypeWrapper("5", "+,+", "simple,fat", "-4,-4,3,3,3|2,5,2,4,4", "simple:-4,2,-1;fat:2", "0-5", "0-1");
}

void SortedDocidRangeSearcherTest::TestCaseForBound()
{
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:4~3", "0-4", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:-1~-2", "0-4", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:2~0", "0-4", "0-4");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:3~-2", "0-4", "0-4");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:2~1", "0-4", "0-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:3~1", "0-4", "0-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:2", "1-3", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0", "simple:0", "0-3", "");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0", "simple:14~12", "0-10", "");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0", "simple:5~9", "0-10", "0-5");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0", "simple:5~4", "0-10", "4-6");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0", "simple:-3~-2", "0-10", "");
    IntTypeWrapper("5", "+,+", "simple,fat", "-4,-4,3,3,3|2,5,2,4,4", "simple:-4;fat:-2~1", "0-5", "");
    IntTypeWrapper("5", "+,+", "simple,fat", "-4,-4,3,3,3|2,5,2,4,4", "simple:-4;fat:4", "0-5", "");
}

void SortedDocidRangeSearcherTest::CreateSegs(const string& segsStr, vector<uint32_t>& docCounts)
{
    // 10,100,200
    StringTokenizer st;
    st.tokenize(segsStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        docCounts.push_back(StringUtil::fromString<uint32_t>(st[i]));
    }
}

void SortedDocidRangeSearcherTest::CreateSortTypes(const string& sortTypesStr,
                                                   vector<indexlibv2::config::SortPattern>& sortTypes)
{
    StringTokenizer st;
    // +,-,+,~
    st.tokenize(sortTypesStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        if (st[i] == "+") {
            sortTypes.push_back(indexlibv2::config::sp_asc);
        } else if (st[i] == "-") {
            sortTypes.push_back(indexlibv2::config::sp_desc);
        } else {
            sortTypes.push_back(indexlibv2::config::sp_nosort);
        }
    }
}

void SortedDocidRangeSearcherTest::CreateAttrNames(const string& attrNamesStr, vector<string>& attrNames)
{
    StringTokenizer st;
    // price,sales,sample
    st.tokenize(attrNamesStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        attrNames.push_back(st[i]);
    }
}

void SortedDocidRangeSearcherTest::CreateDimensionDescriptionVector(
    const string& dimensionDescriptionStr, table::DimensionDescriptionVector& dimensionDescriptions)
{
    StringTokenizer st;
    // a:1,2,3,4~8,^~10 ; b:1,2,3~5,^~^

    st.tokenize(dimensionDescriptionStr, ";", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        std::shared_ptr<table::DimensionDescription> dimension(new table::DimensionDescription);
        StringTokenizer nameSt;
        nameSt.tokenize(st[i], ":", mTokenizeFlag);
        assert(nameSt.getNumTokens() == 2);
        dimension->name = nameSt[0];

        StringTokenizer numsSt;
        numsSt.tokenize(nameSt[1], ",", mTokenizeFlag);

        for (size_t j = 0; j < numsSt.getNumTokens(); ++j) {
            StringTokenizer valuesSt;
            valuesSt.tokenize(numsSt[j], "~", mTokenizeFlag);

            if (valuesSt.getNumTokens() == 1) {
                dimension->values.push_back(valuesSt[0]);
            } else {
                table::DimensionDescription::Range rangeDescription;
                CreateRangeDescriptionFromValueString(valuesSt, rangeDescription);
                dimension->ranges.push_back(rangeDescription);
            }
        }
        dimensionDescriptions.push_back(dimension);
    }
}

void SortedDocidRangeSearcherTest::CreateRangeDescriptionFromValueString(
    const StringTokenizer& tokenizer, table::DimensionDescription::Range& rangeDescription)
{
    if (tokenizer[0] == "^") {
        rangeDescription.from = table::DimensionDescription::Range::INFINITE;
    } else {
        rangeDescription.from = tokenizer[0];
    }

    if (tokenizer[1] == "^") {
        rangeDescription.to = table::DimensionDescription::Range::INFINITE;
    } else {
        rangeDescription.to = tokenizer[1];
    }
}

void SortedDocidRangeSearcherTest::CreateRangeLimit(const string& rangeLimitStr, DocIdRange& rangeLimit)
{
    StringTokenizer st;
    // rangeLimit "1-3"
    st.tokenize(rangeLimitStr, "-", mTokenizeFlag);
    assert(st.getNumTokens() == 2);
    rangeLimit.first = StringUtil::fromString<docid_t>(st[0]);
    rangeLimit.second = StringUtil::fromString<docid_t>(st[1]);
}

void SortedDocidRangeSearcherTest::CreateExpectedRanges(const string& expectedRangesStr,
                                                        DocIdRangeVector& expectedRanges)
{
    StringTokenizer st;
    // expectedRange 1-3, 4-5
    st.tokenize(expectedRangesStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer rangeSt;
        rangeSt.tokenize(st[i], "-", mTokenizeFlag);
        DocIdRange range;
        range.first = StringUtil::fromString<docid_t>(rangeSt[0]);
        range.second = StringUtil::fromString<docid_t>(rangeSt[1]);
        expectedRanges.push_back(range);
    }
}
}} // namespace indexlib::index
