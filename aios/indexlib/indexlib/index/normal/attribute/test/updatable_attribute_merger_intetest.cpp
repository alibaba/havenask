#include <autil/StringUtil.h>
#include "indexlib/partition/patch_loader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/attribute/test/updatable_attribute_merger_intetest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UpdatableAttributeMergerInteTest);

UpdatableAttributeMergerInteTest::UpdatableAttributeMergerInteTest()
{
}

UpdatableAttributeMergerInteTest::~UpdatableAttributeMergerInteTest()
{
}

void UpdatableAttributeMergerInteTest::CaseSetUp()
{
}

void UpdatableAttributeMergerInteTest::CaseTearDown()
{
}

void UpdatableAttributeMergerInteTest::TestSimpleProcessLongCaseTest()
{
    InnerTestAttributeMerge(100, 5, 10, true);
    InnerTestAttributeMerge(50, 5, 20, false);
}

void UpdatableAttributeMergerInteTest::InnerTestAttributeMerge(
        uint32_t docCount, uint32_t docCountPerSegment, 
        uint32_t updateRound, bool multiValue)
{
    TearDown();
    SetUp();

    assert(updateRound <= docCount);

    vector<string> expectValues;
    string mergeFootPrint = "";

    string attrTypeStr = "int32";
    if (multiValue)
    {
        attrTypeStr += ":true:true";
    }

    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), attrTypeStr, SFP_ATTRIBUTE);
    string fullDocStr = PrepareFullDocs(docCount, 
            docCountPerSegment, expectValues);
    provider.Build(fullDocStr, SFP_OFFLINE);

    CheckResult(provider, expectValues, mergeFootPrint);
    for (uint32_t i = 0; i < updateRound; i++)
    {
        string incDocStr = PrepareUpdateDocs(expectValues, 
                docCountPerSegment, i);
        provider.Build(incDocStr, SFP_OFFLINE);
        CheckResult(provider, expectValues, mergeFootPrint);

        Version onDiskVersion = provider.GetPartitionData()->GetOnDiskVersion();
        string mergeParamStr = MakeRandomMergeStrategy(
                i, onDiskVersion, mergeFootPrint);
        provider.Merge(mergeParamStr);
        CheckResult(provider, expectValues, mergeFootPrint);
    }

    cout << "mergeFootPrint is:" << mergeFootPrint << endl;
}

void UpdatableAttributeMergerInteTest::CheckResult(
        SingleFieldPartitionDataProvider& provider,
        const vector<string>& expectValues,
        const std::string& mergeFootPrint)
{
    string failTrace = "CheckResult failed! Merge foot print is "
                       "[" + mergeFootPrint + "]";
    SCOPED_TRACE(failTrace);
    // prepare attr reader
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeReaderPtr attrReader(
            AttributeReaderFactory::CreateAttributeReader(
                    attrConfig, partitionData));
    PatchLoader::LoadAttributePatch(
            *attrReader, attrConfig, partitionData);
    
    // prepare pk index reader
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    IndexConfigPtr pkIndexConfig = 
        schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                    pkIndexConfig->GetIndexType()));
    indexReader->Open(pkIndexConfig, partitionData);
    PrimaryKeyIndexReaderPtr pkIndexReader = 
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexReader, indexReader);

    for (size_t i = 0; i < expectValues.size(); i++)
    {
        string pkStr = StringUtil::toString(i);
        docid_t docId = pkIndexReader->Lookup(pkStr);
        ASSERT_TRUE(docId != INVALID_DOCID);

        string value;
        attrReader->Read(docId, value);
        ASSERT_EQ(expectValues[i], value);
    }
}

string UpdatableAttributeMergerInteTest::PrepareFullDocs(
        uint32_t docCount, uint32_t docCountPerSegment, 
        vector<string>& expectValues)
{
    int32_t initValue = 0;
    expectValues.resize(docCount, StringUtil::toString(initValue));

    stringstream ss;
    for (size_t i = 1; i <= docCount; i++)
    {
        ss << initValue;
        if (i == docCount)
        {
            break;
        }

        if (i % docCountPerSegment == 0)
        {
            ss << "#";
        }
        else
        {
            ss << ",";
        }
    }
    return ss.str();
}

string UpdatableAttributeMergerInteTest::PrepareUpdateDocs(
        vector<string>& expectValues, uint32_t docCountPerSegment, 
        uint32_t updateRoundIdx)
{
    int32_t updateValue = updateRoundIdx + 1;
    size_t docCount = expectValues.size();
    
    stringstream ss;
    size_t count = 0;
    for (size_t i = updateRoundIdx; i < docCount; i++)
    {
        expectValues[i] = StringUtil::toString(updateValue);
        ss << "update_field:" << i << ":" << expectValues[i];
        
        count++;
        if (i == docCount - 1)
        {
            break;
        }

        if (count % docCountPerSegment == 0)
        {
            ss << "#";
        }
        else
        {
            ss << ",";
        }
    }
    return ss.str();
}

string UpdatableAttributeMergerInteTest::MakeRandomMergeStrategy(
        size_t updateRound, const Version& onDiskVersion, 
        string& mergeFootPrint)
{
    size_t segmentCount = onDiskVersion.GetSegmentCount();
    size_t mergedSegmentCount = segmentCount / 5;
    if (mergedSegmentCount == 0)
    {
        mergedSegmentCount = 1;
    }
    vector<segmentid_t> mergeSegId;
    size_t mergeType = updateRound % 3;
    if (mergeType == 0)
    {
        // merge single segment
        mergeSegId.push_back(onDiskVersion[random() % segmentCount]);
    }
    else if (mergeType == 1)
    {
        // merge near segment
        size_t startIdx = random() % mergedSegmentCount;
        for (size_t i = startIdx; i < mergedSegmentCount; i++)
        {
            mergeSegId.push_back(onDiskVersion[i]);
        }
    }
    else if (mergeType == 2)
    {
        // merge random segment
        for (size_t i = 0; i < mergedSegmentCount; i++)
        {
            segmentid_t segId = onDiskVersion[random() % segmentCount];
            while (find(mergeSegId.begin(), mergeSegId.end(), segId) != mergeSegId.end())
            {
                segId = onDiskVersion[random() % segmentCount];
            }
            mergeSegId.push_back(segId);
        }
    }
    sort(mergeSegId.begin(), mergeSegId.end());
    string mergeStr = StringUtil::toString(mergeSegId, ",");
    if (mergeFootPrint.empty())
    {
        mergeFootPrint = mergeStr;
    }
    else
    {
        mergeFootPrint = mergeFootPrint + "@" + mergeStr;
    }
    return mergeStr;
}

IE_NAMESPACE_END(index);

