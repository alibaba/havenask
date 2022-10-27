#include "indexlib/index/normal/test/sorted_docid_range_searcher_unittest.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

void SortedDocidRangeSearcherTest::CaseSetUp()
{
    mRoot = GET_TEST_DATA_PATH();
    mTokenizeFlag = StringTokenizer::TOKEN_TRIM
                    | StringTokenizer::TOKEN_IGNORE_EMPTY;
}

void SortedDocidRangeSearcherTest::CaseTearDown()
{
}

void SortedDocidRangeSearcherTest::FloatTypeWrapper(const string& segsStr,
        const string& sortTypesStr,
        const string& attrNamesStr,
        const string& attrValuesStr,
        const string& dimensStr,
        const string& rangeLimitStr,
        const string& expectedRangesStr)
{
    InternalTest<float>(segsStr, sortTypesStr, attrNamesStr,
                        attrValuesStr, dimensStr, rangeLimitStr,
                        expectedRangesStr);
    InternalTest<double>(segsStr, sortTypesStr, attrNamesStr,
                         attrValuesStr, dimensStr, rangeLimitStr,
                         expectedRangesStr);
}

void SortedDocidRangeSearcherTest::IntTypeWrapper(const string& segsStr,
        const string& sortTypesStr,
        const string& attrNamesStr,
        const string& attrValuesStr,
        const string& dimensStr,
        const string& rangeLimitStr,
        const string& expectedRangesStr)
{
    InternalTest<int8_t>(segsStr, sortTypesStr, attrNamesStr,
                         attrValuesStr, dimensStr, rangeLimitStr,
                         expectedRangesStr);
    InternalTest<int16_t>(segsStr, sortTypesStr, attrNamesStr,
                          attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<int32_t>(segsStr, sortTypesStr, attrNamesStr,
                          attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<int64_t>(segsStr, sortTypesStr, attrNamesStr,
                          attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
}

void SortedDocidRangeSearcherTest::UIntTypeWrapper(const string& segsStr,
        const string& sortTypesStr,
        const string& attrNamesStr,
        const string& attrValuesStr,
        const string& dimensStr,
        const string& rangeLimitStr,
        const string& expectedRangesStr)
{
    InternalTest<uint8_t>(segsStr, sortTypesStr, attrNamesStr,
                          attrValuesStr, dimensStr, rangeLimitStr,
                          expectedRangesStr);
    InternalTest<uint16_t>(segsStr, sortTypesStr, attrNamesStr,
                           attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
    InternalTest<uint32_t>(segsStr, sortTypesStr, attrNamesStr,
                           attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
    InternalTest<uint64_t>(segsStr, sortTypesStr, attrNamesStr,
                           attrValuesStr, dimensStr, rangeLimitStr,
                           expectedRangesStr);
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeOneSegmentASC()
{
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5",
                   "simple:-2~5", "0-10", "2-10");
    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9",
                    "simple:2~5", "0-10", "2-6");
    FloatTypeWrapper("8", "+", "simple", 
                     "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1",
                     "simple:-2.1~2.1", "0-8", "1-7");

    IntTypeWrapper("6", "+", "simple", "-2,-1,-1,1,1,2",
                   "simple:-1~1", "0-6", "1-5");
    UIntTypeWrapper("4", "+", "simple", "0,1,1,2",
                    "simple:1~1", "0-4", "1-3");
    FloatTypeWrapper("5", "+", "simple", "-1.1,1.1,2.2,2.2,3.3",
                     "simple:2.2", "0-5", "2-4");
}
void SortedDocidRangeSearcherTest::TestCaseForINFINITE()
{        
    IntTypeWrapper("4", "+", "simple", "-2,-1,-1,2",
                   "simple:^~^", "0-4", "0-4");
    FloatTypeWrapper("5", "+", "simple", "1.1,2.2,2.2,2.2,3.3",
                     "simple:2.2~^", "0-5", "1-5");
    FloatTypeWrapper("5", "-", "simple", "2.2,1.1,-2.2,-2.2,-2.2",
                     "simple:^~-2.2", "0-5", "0-5");
}

void SortedDocidRangeSearcherTest::TestCaseForRangeSwap()
{
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5",
                   "simple:-2~5", "0-10", "2-10");
    IntTypeWrapper("10", "+", "simple", "-4,-3,-2,-1,0,1,2,3,4,5",
                   "simple:5~-2", "0-10", "2-10");
    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9",
                    "simple:2~5", "0-10", "2-6");
    UIntTypeWrapper("10", "+", "simple", "0,1,2,3,4,5,6,7,8,9",
                    "simple:5~2", "0-10", "2-6");
    FloatTypeWrapper("8", "+", "simple", 
                     "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1",
                     "simple:-2.1~2.1", "0-8", "1-7");
    FloatTypeWrapper("8", "+", "simple", 
                     "-3.1,-2.1,-1.1,-0.1,0.1,1.1,2.1,3.1",
                     "simple:2.1~-2.1", "0-8", "1-7");
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeOneSegmentDESC()
{
    IntTypeWrapper("4", "-", "simple", "2,-1,-1,-2",
                   "simple:-1~-1", "0-4", "1-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:1~1", "0-4", "1-3");
    FloatTypeWrapper("5", "-", "simple", "3.3,2.2,2.2,2.2,1.1",
                     "simple:2.2", "0-5", "1-4");

    IntTypeWrapper("10", "-", "simple", "9,8,7,6,5,4,3,2,1,0",
                   "simple:2~5", "0-10", "4-8");
    FloatTypeWrapper("8", "-", "simple", 
                     "3.1,2.1,1.1,0.1,-0.1,-1.1,-2.1,-3.1",
                     "simple:-2.1~2.1", "0-8", "1-7");
    FloatTypeWrapper("8", "-", "simple", 
                     "3.1,2.1,1.1,0.1,-0.1,-1.1,-2.1,-3.1",
                     "simple:-2.1~2.1,-3.1,4.1~3.1", "0-8", 
                     "0-1,1-7,7-8");
}

void SortedDocidRangeSearcherTest::TestCaseForOneAttributeMultiSegment()
{
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0",
                   "simple:7~2", "0-10", "2-8");
    IntTypeWrapper("5,5", "+", "simple", "0,1,2,3,4;5,6,7,8,9",
                   "simple:7~2", "0-10", "2-8");
    IntTypeWrapper("5,5,5", "+", "simple", 
                   "-4,-4,-2,-2,-1;0,1,2,3,4;5,7,7,8,8",
                   "simple:7~-2", "0-15", "2-13");
}

void SortedDocidRangeSearcherTest::TestCaseForMultiAttributeOneSegment()
{
    IntTypeWrapper("5", "+,+", "simple,fat", 
                   "-4,-4,3,3,3|2,5,2,4,4",
                   "simple:-4,3;fat:1~8", "0-5", "0-2,2-5");
    IntTypeWrapper("5", "+,+", "simple,fat", 
                   "-4,-4,3,3,3|2,5,2,4,4",
                   "simple:-4,2,-1;fat:5", "0-5", "1-2");
    IntTypeWrapper("5", "+,+", "simple,fat", 
                   "-4,-4,3,3,3|2,5,2,4,4",
                   "simple:-4,2,-1;fat:2", "0-5", "0-1");
}

void SortedDocidRangeSearcherTest::TestCaseForBound()
{
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:4~3", "0-4", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:-1~-2", "0-4", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:2~0", "0-4", "0-4");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:3~-2", "0-4", "0-4");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:2~1", "0-4", "0-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:3~1", "0-4", "0-3");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:2", "1-3", "");
    IntTypeWrapper("4", "-", "simple", "2,1,1,0",
                   "simple:0", "0-3", "");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0",
                   "simple:14~12", "0-10", "");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0",
                   "simple:5~9", "0-10", "0-5");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0",
                   "simple:5~4", "0-10", "4-6");
    IntTypeWrapper("5,5", "-", "simple", "9,8,7,6,5;4,3,2,1,0",
                   "simple:-3~-2", "0-10", "");
    IntTypeWrapper("5", "+,+", "simple,fat", 
                   "-4,-4,3,3,3|2,5,2,4,4",
                   "simple:-4;fat:-2~1", "0-5", "");
    IntTypeWrapper("5", "+,+", "simple,fat", 
                   "-4,-4,3,3,3|2,5,2,4,4",
                   "simple:-4;fat:4", "0-5", "");
}


void SortedDocidRangeSearcherTest::CreateSegs(const string& segsStr, vector<uint32_t>& docCounts) {
    // 10,100,200
    StringTokenizer st;
    st.tokenize(segsStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        docCounts.push_back(StringUtil::fromString<uint32_t>(st[i]));
    }
}

void SortedDocidRangeSearcherTest::CreateSortTypes(const string& sortTypesStr, vector<SortPattern>& sortTypes) {
    StringTokenizer st;
    // +,-,+,~
    st.tokenize(sortTypesStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        if (st[i] == "+") {
            sortTypes.push_back(sp_asc);
        } else if (st[i] == "-") {
            sortTypes.push_back(sp_desc);
        } else {
            sortTypes.push_back(sp_nosort);
        }
    }
}

void SortedDocidRangeSearcherTest::CreateAttrNames(const string& attrNamesStr, vector<string>& attrNames) {
    StringTokenizer st;
    // price,sales,sample
    st.tokenize(attrNamesStr, ",", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        attrNames.push_back(st[i]);
    }
}


void SortedDocidRangeSearcherTest::CreateDimensionDescriptionVector(
        const string& dimensionDescriptionStr, 
        DimensionDescriptionVector& dimensionDescriptions) 
{
    StringTokenizer st;
    // a:1,2,3,4~8,^~10 ; b:1,2,3~5,^~^

    st.tokenize(dimensionDescriptionStr, ";", mTokenizeFlag);
    for (size_t i = 0; i < st.getNumTokens(); ++i) 
    {
        DimensionDescriptionPtr dimension(new DimensionDescription);
        StringTokenizer nameSt;
        nameSt.tokenize(st[i], ":", mTokenizeFlag);
        assert(nameSt.getNumTokens() == 2);
        dimension->name = nameSt[0];

        StringTokenizer numsSt;
        numsSt.tokenize(nameSt[1], ",", mTokenizeFlag);

        for (size_t j = 0; j < numsSt.getNumTokens(); ++j) 
        {
            StringTokenizer valuesSt;
            valuesSt.tokenize(numsSt[j], "~", mTokenizeFlag);

            if (valuesSt.getNumTokens() == 1) 
            {
                dimension->values.push_back(valuesSt[0]);
            } 
            else 
            {
                index_base::RangeDescription rangeDescription;
                CreateRangeDescriptionFromValueString(
                        valuesSt, rangeDescription);
                dimension->ranges.push_back(rangeDescription);
            }
        }
        dimensionDescriptions.push_back(dimension);
    }
}

void SortedDocidRangeSearcherTest::CreateRangeDescriptionFromValueString(
        const StringTokenizer& tokenizer,
        index_base::RangeDescription& rangeDescription)
{
    if (tokenizer[0] == "^") 
    {
        rangeDescription.from = index_base::RangeDescription::INFINITE;
    } 
    else 
    {
        rangeDescription.from = tokenizer[0];
    }

    if (tokenizer[1] == "^") 
    {
        rangeDescription.to = index_base::RangeDescription::INFINITE;
    } 
    else 
    {
        rangeDescription.to = tokenizer[1];
    }
}

void SortedDocidRangeSearcherTest::CreateRangeLimit(const string& rangeLimitStr,
        DocIdRange& rangeLimit) 
{
    StringTokenizer st;
    // rangeLimit "1-3"
    st.tokenize(rangeLimitStr, "-", mTokenizeFlag);
    assert(st.getNumTokens() == 2);
    rangeLimit.first = StringUtil::fromString<docid_t>(st[0]);
    rangeLimit.second = StringUtil::fromString<docid_t>(st[1]);
}

void SortedDocidRangeSearcherTest::CreateExpectedRanges(const string& expectedRangesStr, 
        DocIdRangeVector& expectedRanges) {
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

IE_NAMESPACE_END(index);
