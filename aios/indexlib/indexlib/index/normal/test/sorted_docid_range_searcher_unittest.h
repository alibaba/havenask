#include <autil/StringTokenizer.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index/normal/attribute/test/single_value_attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index/test/index_test_util.h"

IE_NAMESPACE_BEGIN(index);

class SortedDocidRangeSearcherTest : public INDEXLIB_TESTBASE
{
public:
    SortedDocidRangeSearcherTest() {}
    ~SortedDocidRangeSearcherTest() {}

    DECLARE_CLASS_NAME(SortedDocidRangeSearcherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForOneAttributeOneSegmentASC();
    void TestCaseForINFINITE();
    void TestCaseForRangeSwap();
    void TestCaseForOneAttributeOneSegmentDESC();
    void TestCaseForOneAttributeMultiSegment();
    void TestCaseForMultiAttributeOneSegment();
    void TestCaseForBound();

private:
    void FloatTypeWrapper(const std::string& segsStr,
                          const std::string& sortTypesStr,
                          const std::string& attrNamesStr,
                          const std::string& attrValuesStr,
                          const std::string& dimensStr,
                          const std::string& rangeLimitStr,
                          const std::string& expectedRangesStr);

    void IntTypeWrapper(const std::string& segsStr,
                        const std::string& sortTypesStr,
                        const std::string& attrNamesStr,
                        const std::string& attrValuesStr,
                        const std::string& dimensStr,
                        const std::string& rangeLimitStr,
                        const std::string& expectedRangesStr);

    void UIntTypeWrapper(const std::string& segsStr,
                         const std::string& sortTypesStr,
                         const std::string& attrNamesStr,
                         const std::string& attrValuesStr,
                         const std::string& dimensStr,
                         const std::string& rangeLimitStr,
                         const std::string& expectedRangesStr);




    void CreateSegs(const std::string& segsStr, std::vector<uint32_t>& docCounts);
    void CreateSortTypes(const std::string& sortTypesStr, std::vector<SortPattern>& sortTypes);
    void CreateAttrNames(const std::string& attrNamesStr, std::vector<std::string>& attrNames);

    template<typename T>
    void CreateAttrValues(const std::string& attrValuesStr, 
                          std::vector < std::vector< std::vector<T> > > &datas) {
        autil::StringTokenizer st;
        //multi_attr -> multi_seg -> attr_values
        // 1,1,2;2,3,4 | 2,3,4;5,6,7
        st.tokenize(attrValuesStr, "|", mTokenizeFlag);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            std::vector< std::vector<T> > attrDatas;
            autil::StringTokenizer multiSegSt;
            multiSegSt.tokenize(st[i], ";", mTokenizeFlag);
            for (size_t j = 0; j < multiSegSt.getNumTokens(); ++j) {
                std::vector<T> segDatas;
                autil::StringTokenizer valuesSt;
                valuesSt.tokenize(multiSegSt[j], ",", mTokenizeFlag);
                for (size_t k = 0; k < valuesSt.getNumTokens(); ++k) {
                    segDatas.push_back(autil::StringUtil::fromString<T>(valuesSt[k]));
                }
                attrDatas.push_back(segDatas);
            }
            datas.push_back(attrDatas);
        }
    }

    void CreateDimensionDescriptionVector(
            const std::string& dimensionDescriptionStr, 
            index_base::DimensionDescriptionVector& dimensionDescriptions);

    void CreateRangeDescriptionFromValueString(
            const autil::StringTokenizer& tokenizer,
            index_base::RangeDescription& rangeDescription);
    void CreateRangeLimit(const std::string& rangeLimitStr,
                          DocIdRange& rangeLimit);
    void CreateExpectedRanges(const std::string& expectedRangesStr, 
                              DocIdRangeVector& expectedRanges);

    template<typename T>
    void InternalTest(const std::string& segsStr,
                      const std::string& sortTypesStr,
                      const std::string& attrNamesStr,
                      const std::string& attrValuesStr,
                      const std::string& dimensStr,
                      const std::string& rangeLimitStr,
                      const std::string& expectedRangesStr)
    {
        std::vector<uint32_t> docCounts;
        CreateSegs(segsStr, docCounts);

        std::vector<SortPattern> sortTypes;
        CreateSortTypes(sortTypesStr, sortTypes);

        std::vector<std::string> attrNames;
        CreateAttrNames(attrNamesStr, attrNames);

        std::vector <std::vector<std::vector<T> > > datas;
        CreateAttrValues(attrValuesStr, datas);

        index_base::DimensionDescriptionVector dimensionDescriptions;
        CreateDimensionDescriptionVector(dimensStr, dimensionDescriptions);

        DocIdRange rangeLimit;
        CreateRangeLimit(rangeLimitStr, rangeLimit);

        DocIdRangeVector expectedRanges;
        CreateExpectedRanges(expectedRangesStr, expectedRanges);

        index_base::PartitionMetaPtr meta(new index_base::PartitionMeta);
        for (size_t i = 0; i < attrNames.size(); ++i) {
            meta->AddSortDescription(attrNames[i], sortTypes[i]);
        }

        SingleValueAttributeReaderCreator<T> attrCreator(
                GET_PARTITION_DIRECTORY(), meta);
        MultiFieldAttributeReaderPtr attrReaders(
                new MultiFieldAttributeReader(config::AttributeSchemaPtr()));
        for (size_t i = 0; i < attrNames.size(); ++i) {
            std::tr1::shared_ptr<SingleValueAttributeReader<T> > reader = 
                attrCreator.Create(docCounts, datas[i], attrNames[i]);
            attrReaders->AddAttributeReader(attrNames[i], reader);
        }

        mSortedDocidRangeSearcher.reset(new SortedDocidRangeSearcher);
        mSortedDocidRangeSearcher->Init(attrReaders, *meta);

        DocIdRangeVector resultRanges;
        mSortedDocidRangeSearcher->GetSortedDocIdRanges(
                dimensionDescriptions, rangeLimit, resultRanges);

        INDEXLIB_TEST_EQUAL(expectedRanges.size(), resultRanges.size());
        for (size_t i = 0; i < expectedRanges.size(); ++i) {
            const DocIdRange& expectedRange = expectedRanges[i];
            INDEXLIB_TEST_EQUAL(expectedRange.first, 
                    resultRanges[i].first);
            INDEXLIB_TEST_EQUAL(expectedRange.second, 
                    resultRanges[i].second);
        }
    }

private:
    SortedDocidRangeSearcherPtr mSortedDocidRangeSearcher;
    std::string mRoot;
    int32_t mTokenizeFlag;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SortedDocidRangeSearcherTest);

INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForOneAttributeOneSegmentASC);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForOneAttributeOneSegmentDESC);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForBound);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForRangeSwap);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForINFINITE);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForOneAttributeMultiSegment);
INDEXLIB_UNIT_TEST_CASE(SortedDocidRangeSearcherTest, TestCaseForMultiAttributeOneSegment);

IE_NAMESPACE_END(index);
