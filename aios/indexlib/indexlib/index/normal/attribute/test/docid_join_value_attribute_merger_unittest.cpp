#include "indexlib/index/normal/attribute/test/docid_join_value_attribute_merger_unittest.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, DocidJoinValueAttributeMergerTest);

DocidJoinValueAttributeMergerTest::DocidJoinValueAttributeMergerTest()
{
}

DocidJoinValueAttributeMergerTest::~DocidJoinValueAttributeMergerTest()
{
}

void DocidJoinValueAttributeMergerTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();

    mMainJoinAttrConfig.reset(new AttributeConfig);
    mMainJoinAttrConfig->SetAttrId(0);
    FieldConfigPtr mainJoinFieldConfig(
            new FieldConfig(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, ft_int32, false));
    mMainJoinAttrConfig->Init(mainJoinFieldConfig);

    mSubJoinAttrConfig.reset(new AttributeConfig);
    mSubJoinAttrConfig->SetAttrId(0);
    FieldConfigPtr subJoinFieldConfig(
            new FieldConfig(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, ft_int32, false));
    mSubJoinAttrConfig->Init(subJoinFieldConfig);
}

void DocidJoinValueAttributeMergerTest::CaseTearDown()
{
}

ReclaimMapPtr DocidJoinValueAttributeMergerTest::CreateReclaimMap(
    uint32_t segDocCount, docid_t offset)
{
    ReclaimMapPtr reclaimMap(new ReclaimMap);

    vector<docid_t> joinValueArrays;
    for (size_t i = 0; i < segDocCount; ++i)
    {
        joinValueArrays.push_back((docid_t)i + offset);
    }
    reclaimMap->InitJoinValueArray(joinValueArrays);

    return reclaimMap;
}

void DocidJoinValueAttributeMergerTest::CheckResult(
    const std::string& segName, uint32_t segDocCount, bool isMain, docid_t offset)
{
    string attrDir = mRootDir + segName + "/";
    if (isMain) 
    {
        attrDir += mMainJoinAttrConfig->GetAttrName() + "/";
    }
    else 
    {
        attrDir += mSubJoinAttrConfig->GetAttrName() + "/";
    }
    INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(attrDir)) << attrDir;

    string dataFilePath = attrDir + ATTRIBUTE_DATA_FILE_NAME;
    INDEXLIB_TEST_EQUAL(FileSystemWrapper::GetFileLength(dataFilePath), 
                        segDocCount * sizeof(docid_t));

    FileNodePtr dataFile(InMemFileNodeCreator::Create(dataFilePath));
    vector<docid_t> joinAttrValues;
    joinAttrValues.resize(segDocCount);
    dataFile->Read(joinAttrValues.data(), segDocCount * sizeof(docid_t), 0);
    dataFile->Close();
    for (size_t i = 0; i < segDocCount; i++)
    {
        INDEXLIB_TEST_EQUAL((docid_t)i + offset, joinAttrValues[i])
            << segName << " no:" << i << " isMain:" << isMain << " offset:" << offset;
    }
}

vector<docid_t> DocidJoinValueAttributeMergerTest::LoadJoinValues(
    const std::string& segName, bool isMain)
{
    string attrDir = mRootDir + segName + "/";
    if (isMain) 
    {
        attrDir += mMainJoinAttrConfig->GetAttrName() + "/";
    }
    else 
    {
        attrDir += mSubJoinAttrConfig->GetAttrName() + "/";
    }
    EXPECT_TRUE(FileSystemWrapper::IsExist(attrDir)) << attrDir;

    string dataFilePath = attrDir + ATTRIBUTE_DATA_FILE_NAME;
    auto fileLen = FileSystemWrapper::GetFileLength(dataFilePath);

    FileNodePtr dataFile(InMemFileNodeCreator::Create(dataFilePath));
    vector<docid_t> joinAttrValues;
    joinAttrValues.resize(fileLen / sizeof(docid_t));
    dataFile->Read(joinAttrValues.data(), fileLen, 0);
    dataFile->Close();
    return joinAttrValues;
}

void DocidJoinValueAttributeMergerTest::TestMerge()
{
    SegmentMergeInfos segMergeInfos;
    uint32_t segDocCount = 10;
    ReclaimMapPtr reclaimMap = CreateReclaimMap(segDocCount);

    DocidJoinValueAttributeMerger docidJoinValueAttributeMerger;
    docidJoinValueAttributeMerger.Init(mMainJoinAttrConfig);

    MergerResource resource;
    resource.targetSegmentCount = 1;
    resource.reclaimMap = reclaimMap;
    OutputSegmentMergeInfo segInfo;
    segInfo.targetSegmentIndex = 0;
    segInfo.path = mRootDir;
    segInfo.directory = DirectoryCreator::Create(segInfo.path);
    docidJoinValueAttributeMerger.Merge(resource, segMergeInfos, {segInfo});

    CheckResult("", segDocCount);
}

void DocidJoinValueAttributeMergerTest::TestMergeToMultiSegment()
{

    DoTestMergeToMultiSegment(true);
    DoTestMergeToMultiSegment(true, 1);
    DoTestMergeToMultiSegment(true, 3);
}

void DocidJoinValueAttributeMergerTest::TestSubMergeToMultiSegment()
{
    DoTestMergeToMultiSegment(false);
    DoTestMergeToMultiSegment(false, 1);
    DoTestMergeToMultiSegment(false, 3);
}

void DocidJoinValueAttributeMergerTest::DoTestMergeToMultiSegment(bool isMain, docid_t offset)
{
    TearDown();
    SetUp();
    uint32_t segDocCount = 10;
    ReclaimMapPtr reclaimMap = CreateReclaimMap(segDocCount, offset);
    reclaimMap->TEST_SetTargetBaseDocIds({ 0, 5 });
    if (isMain)
    {
        DoMergeMultiSegment(isMain, reclaimMap, {});
        CheckResult("seg0", 5, isMain, offset);
        CheckResult("seg1", 5, isMain, 1);
    }
    else
    {
        DoMergeMultiSegment(isMain, reclaimMap, { 0, 5 + offset });
        CheckResult("seg0", 5, isMain, offset);
        CheckResult("seg1", 5, isMain, 0);
    }
}

void DocidJoinValueAttributeMergerTest::TestMergeSpecialJoinValue() {
    {
        TearDown();
        SetUp();
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        vector<docid_t> joinValueArrays = { 1, 3, 3, 4, 5 };
        reclaimMap->InitJoinValueArray(joinValueArrays);
        reclaimMap->TEST_SetTargetBaseDocIds({ 0, 2 });
        DoMergeMultiSegment(true, reclaimMap);
        auto seg0Actual = LoadJoinValues("seg0", true);
        auto seg1Actual = LoadJoinValues("seg1", true);
        vector<docid_t> seg0Expected = { 1, 3 };
        vector<docid_t> seg1Expected = { 0, 1, 2 };

        ASSERT_EQ(seg0Expected, seg0Actual);
        ASSERT_EQ(seg1Expected, seg1Actual);
    }
    {
        TearDown();
        SetUp();
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        vector<docid_t> joinValueArrays = { 1, 2, 2, 4, 5 };
        reclaimMap->InitJoinValueArray(joinValueArrays);
        reclaimMap->TEST_SetTargetBaseDocIds({ 0, 3 });
        DoMergeMultiSegment(false, reclaimMap, {0, 3});
        auto seg0Actual = LoadJoinValues("seg0", false);
        auto seg1Actual = LoadJoinValues("seg1", false);
        vector<docid_t> seg0Expected = { 1, 2, 2 };
        vector<docid_t> seg1Expected = { 1, 2 };

        ASSERT_EQ(seg0Expected, seg0Actual);
        ASSERT_EQ(seg1Expected, seg1Actual);
    }
    {
        TearDown();
        SetUp();
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        vector<docid_t> joinValueArrays = { 0, 2, 2, 4, 4 };
        reclaimMap->InitJoinValueArray(joinValueArrays);
        reclaimMap->TEST_SetTargetBaseDocIds({ 0, 3 });
        DoMergeMultiSegment(false, reclaimMap, {0, 4});
        auto seg0Actual = LoadJoinValues("seg0", false);
        auto seg1Actual = LoadJoinValues("seg1", false);
        vector<docid_t> seg0Expected = { 0, 2, 2 };
        vector<docid_t> seg1Expected = { 0, 0 };

        ASSERT_EQ(seg0Expected, seg0Actual);
        ASSERT_EQ(seg1Expected, seg1Actual);
    }        
}

void DocidJoinValueAttributeMergerTest::DoMergeMultiSegment(
    bool isMain, const ReclaimMapPtr& reclaimMap, const std::vector<docid_t>& mainBaseDocIds)
{
    SegmentMergeInfos segMergeInfos;
    DocidJoinValueAttributeMerger docidJoinValueAttributeMerger;
    if (isMain)
    {
        docidJoinValueAttributeMerger.Init(mMainJoinAttrConfig);
    }
    else
    {
        docidJoinValueAttributeMerger.Init(mSubJoinAttrConfig);
    }

    MergerResource resource;
    resource.targetSegmentCount = 2;
    resource.reclaimMap = reclaimMap;
    resource.mainBaseDocIds = mainBaseDocIds;
    OutputSegmentMergeInfos segInfos;
    {
        OutputSegmentMergeInfo segInfo;
        segInfo.targetSegmentIndex = 0;
        segInfo.path = FileSystemWrapper::JoinPath(mRootDir, "seg0");
        segInfo.directory = DirectoryCreator::Create(segInfo.path);
        segInfos.push_back(segInfo);
    }
    {
        OutputSegmentMergeInfo segInfo;
        segInfo.targetSegmentIndex = 1;
        segInfo.path = FileSystemWrapper::JoinPath(mRootDir, "seg1");
        segInfo.directory = DirectoryCreator::Create(segInfo.path);
        segInfos.push_back(segInfo);
    }
    docidJoinValueAttributeMerger.Merge(resource, segMergeInfos, segInfos);
}

IE_NAMESPACE_END(index);

