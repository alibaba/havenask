#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "attribute_merger_test_util.h"
#include "updatable_var_num_attribute_merger_test_util.h"
#include "updatable_var_num_attribute_merger_unittest.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;

using testing::NiceMock;
using testing::_;
using testing::Field;
using testing::AtMost;
using testing::Return;
using testing::Invoke;
using testing::SetArgReferee;
using testing::SetArrayArgument;
using testing::DoAll;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UpdatableVarNumAttributeMergerTest);

template <typename T>
class UpdatableVarNumAttributeMergerForTest : public VarNumAttributeMerger<T>
{
public:
    UpdatableVarNumAttributeMergerForTest()
        : VarNumAttributeMerger<T>(true)
    {}
    ~UpdatableVarNumAttributeMergerForTest()
    {
    }

public:
    MOCK_METHOD1_T(CreateSegmentReader, 
                   AttributeSegmentReaderPtr(const SegmentMergeInfo& segMergeInfo));
    MOCK_METHOD1_T(CreatePatchReaderForSegment, 
                   AttributePatchReaderPtr(segmentid_t segmentId));
    MOCK_METHOD0_T(CreatePatchReader,
                   AttributePatchReaderPtr());
    MOCK_METHOD0_T(CreatePatchMerger,
                   typename VarNumAttributeMerger<T>::PatchMergerPtr());
    MOCK_METHOD3_T(ExtractMergeSegmentIds, void(
                    const SegmentMergeInfos &segMergeInfos,
                    segmentid_t &maxSegmentIdInMerge,
                    std::vector<segmentid_t> &mergeSegmentIdVec));

public:
    const vector<string> &GetActualDataVec() const
    {
        return mActualDataVec;
    }
    const vector<string> &GetActualOffsetVec() const
    {
        return mActualOffsetVec;
    }
    
    AttributePatchReaderPtr RealCreatePatchReaderForSegment(segmentid_t segmentId)
    {
        return VarNumAttributeMerger<T>::CreatePatchReaderForSegment(segmentId);
    }
    
private:
    void CreateFiles(const std::string& dir) override
    {
        this->mFileOffset = 0;
        this->mDataFile.reset(new MockBufferedFileWriter(mActualDataVec));
        this->mOffsetFile.reset(new MockBufferedFileWriter(mActualOffsetVec));
        this->mDataInfoFile.reset(new MockBufferedFileWriter(mActualDataInfoVec));
    }
    void DumpOffsetFile() override
    {
        for (size_t i = 0; i < this->mOffsetDumper->Size(); ++i)
        {
            uint64_t offsetValue = this->mOffsetDumper->GetOffset(i);
            this->mOffsetFile->Write((void *)(&offsetValue), sizeof(uint64_t));
        }
    }

private:
    vector<string> mActualDataVec;
    vector<string> mActualOffsetVec;
    vector<string> mActualDataInfoVec;
};

//////////////////////////////////////////////////////////////////////
UpdatableVarNumAttributeMergerTest::UpdatableVarNumAttributeMergerTest()
{
}

UpdatableVarNumAttributeMergerTest::~UpdatableVarNumAttributeMergerTest()
{
}

void UpdatableVarNumAttributeMergerTest::CaseSetUp()
{
    mTestDataPath = GET_TEST_DATA_PATH();
    IndexFormatVersion formatVersion;
    formatVersion.Set("1.8.0");
    formatVersion.Store(GET_PARTITION_DIRECTORY());
}

void UpdatableVarNumAttributeMergerTest::CaseTearDown()
{
}

/*
 * Format: 
 * dataAndPatchString: REFER TO CreateAttributeReadersAndPatchReaders 
 */
template<typename T>
AttributeMergerPtr UpdatableVarNumAttributeMergerTest::CreateAttributeMerger(
        const string &dataAndPatchString)
{
    vector<AttributeSegmentReaderPtr> attributeReaderVec;
    vector<typename VarNumAttributeTestUtil<T>::PatchReaderPtr> patchReaderVec;
    uint32_t segNum = VarNumAttributeTestUtil<T>::CreateAttributeReadersAndPatchReaders(
            dataAndPatchString, 
            attributeReaderVec, patchReaderVec);

    NiceMock<UpdatableVarNumAttributeMergerForTest<T> > *merger =
        new NiceMock<UpdatableVarNumAttributeMergerForTest<T> >;
    for (size_t segId = 0; segId < segNum; ++segId)
    {
        ON_CALL(*merger, CreateSegmentReader(Field(&SegmentMergeInfo::segmentId, segId)))
            .WillByDefault(Return(attributeReaderVec[segId]));
        ON_CALL(*merger, CreatePatchReaderForSegment(segId))
            .WillByDefault(Return(patchReaderVec[segId]));
    }
    return AttributeMergerPtr(merger);
}

/*
 * Format: 
 * expectDataAndOffsetString  : offset1:v11,v12,...|offset2:v21,...|....
 */
template<typename T>
void UpdatableVarNumAttributeMergerTest::CheckDataAndOffset(
        const AttributeMergerPtr &mergerPtr,
        const string &expectDataAndOffsetString)
{
    NiceMock<UpdatableVarNumAttributeMergerForTest<T> > *merger = 
        (NiceMock<UpdatableVarNumAttributeMergerForTest<T> > *)(mergerPtr.get());
    const vector<string> &actualDataVec = merger->GetActualDataVec();
    const vector<string> &actualOffsetVec = merger->GetActualOffsetVec();
    vector<string> expectDataVec;
    vector<string> expectOffsetVec;
    VarNumAttributeTestUtil<T>::CreateExpectMergedDataAndOffset(
            expectDataAndOffsetString, expectDataVec, expectOffsetVec);
    EXPECT_EQ(expectOffsetVec, actualOffsetVec)
        << "HINT    : [" << expectDataAndOffsetString << "]";
    EXPECT_EQ(expectDataVec, actualDataVec) 
        << "HINT    : [" << expectDataAndOffsetString << "]";
}

/*
 * Format:
 * dataAndPatchString       : REFER TO CreateAttributeReadersAndPatchReaders
 * segMergeInfosString      : REFER TO CreateSegmentMergeInfos
 * expectDataAndOffsetString: REFER TO CreateExpectMergedDataAndOffset
 */
template<typename T>
void UpdatableVarNumAttributeMergerTest::CheckMerge(
        const string &dataAndPatchString,
        const string &segMergeInfosString,
        const string &expectDataAndOffsetString)
{
    AttributeMergerPtr mergerPtr = CreateAttributeMerger<T>(dataAndPatchString);

    ReclaimMapPtr reclaimMap = 
        AttributeMergerTestUtil::CreateReclaimMap(segMergeInfosString);
    SegmentMergeInfos segMergeInfos =
        AttributeMergerTestUtil::CreateSegmentMergeInfos(segMergeInfosString);
    mergerPtr->Init(AttributeMergerTestUtil::CreateAttributeConfig<T>(true));
    Version version;
    SegmentDirectoryPtr segDir(new SegmentDirectory(mTestDataPath, version));
    segDir->Init(false);
    mergerPtr->BeginMerge(segDir);
    mergerPtr->Merge("", segMergeInfos, reclaimMap);

    CheckDataAndOffset<T>(mergerPtr, expectDataAndOffsetString);
}

/*
 * Format:
 * segIdsInCurVersionString: "segId1,segId2,..."
 * segIdsInMergeString     : "segId1,segId2,..."
 * patchesForSegIdString   : "targetSegId:patchSegId1,patchSegId2...;..."
 * expectMergePatchesString: "targetSegId:patchSegId1,patchSegId2...;..."
 */
void UpdatableVarNumAttributeMergerTest::CheckMergePatches(
        const string &segIdsInCurVersionString,
        const string &segIdsInMergeString,
        const string &patchesForSegIdString,
        const string &expectMergePatchesString)
{
    UpdatableVarNumAttributeMergerForTest<int32_t> merger;
    merger.Init(AttributeMergerTestUtil::CreateAttributeConfig<int32_t>(true));
    
    Version version(0);
    StringTokenizer stSegIdInVersion;
    stSegIdInVersion.tokenize(segIdsInCurVersionString, ",");
    for (size_t i = 0; i < stSegIdInVersion.getNumTokens(); ++i)
    {
        version.AddSegment(StringUtil::fromString<segmentid_t>(stSegIdInVersion[i]));
    }

    MockSegmentDirectory *mockSegDir = new MockSegmentDirectory(mTestDataPath, version);
    mockSegDir->SetPartitionData(
            PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM()));
    SegmentDirectoryPtr segDir(mockSegDir);
    merger.BeginMerge(segDir);

    StringTokenizer stMergeSegIdList;
    stMergeSegIdList.tokenize(segIdsInMergeString, ",");
    SegmentMergeInfos segMergeInfos;
    for (size_t i = 0; i < stMergeSegIdList.getNumTokens(); ++i)
    {
        SegmentMergeInfo mergeInfo;
        mergeInfo.segmentId = StringUtil::fromString<segmentid_t>(stMergeSegIdList[i]);
        segMergeInfos.push_back(mergeInfo);
    }

    StringTokenizer stPatchesForSegId;
    stPatchesForSegId.tokenize(patchesForSegIdString, ";",
                               StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < stPatchesForSegId.getNumTokens(); ++i)
    {
        StringTokenizer stPatchesForOneSegId;
        stPatchesForOneSegId.tokenize(stPatchesForSegId[i], ":");
        assert(2 == stPatchesForOneSegId.getNumTokens());
        
        segmentid_t targetSegId = StringUtil::fromString<segmentid_t>(stPatchesForOneSegId[0]);
        vector<pair<string, segmentid_t> > patches;
        StringTokenizer stSegIds;
        stSegIds.tokenize(stPatchesForOneSegId[1], ",",
                          StringTokenizer::TOKEN_IGNORE_EMPTY
                          | StringTokenizer::TOKEN_TRIM);
        for (size_t j = 0; j < stSegIds.getNumTokens(); ++j)
        {
            segmentid_t segId = StringUtil::fromString<segmentid_t>(stSegIds[j]);
            patches.push_back(make_pair(stSegIds[j], segId));
        }
        EXPECT_CALL(*mockSegDir, ListAttrPatch(_, targetSegId, _))
            .Times(AtMost(1))
            .WillRepeatedly(DoAll(SetArgReferee<2>(patches), Return()));
    }

    StringTokenizer stExpectMergePatches;
    stExpectMergePatches.tokenize(expectMergePatchesString, ";",
                               StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < stExpectMergePatches.getNumTokens(); ++i)
    {
        StringTokenizer stMergePatchForOneSegId;
        stMergePatchForOneSegId.tokenize(stExpectMergePatches[i], ":");
        assert(2 == stMergePatchForOneSegId.getNumTokens());
        MockVarNumAttributePatchMerger<int32_t> *mockPatchMerger = 
            new MockVarNumAttributePatchMerger<int32_t>;
        VarNumAttributeMerger<int32_t>::PatchMergerPtr patchMerger(mockPatchMerger);
        EXPECT_CALL(merger, CreatePatchMerger())
            .WillOnce(Return(patchMerger));    
        EXPECT_CALL(*mockPatchMerger, Merge(_, _))
            .WillOnce(Return());
    }    
    merger.MergePatches(mTestDataPath, segMergeInfos);
}

//////////////////////////////////////////////////////////////////////
void UpdatableVarNumAttributeMergerTest::TestSimpleProcess()
{
    MockVarNumAttributePatchReader<int32_t> *mockPatchReader =
        new MockVarNumAttributePatchReader<int32_t>();
    AttributePatchReaderPtr patchReader(mockPatchReader);
    EXPECT_CALL(*mockPatchReader, Seek(_, _, _))
        .WillOnce(Return(0));

    MockUpdatableVarNumAttributeSegmentReader *mockAttributeReader = 
        new MockUpdatableVarNumAttributeSegmentReader;
    AttributeSegmentReaderPtr attributeReader(mockAttributeReader);
    int32_t dataValue[] = {1};
    uint8_t *bytesValue = (uint8_t *) dataValue;
    eEXPECT_CALL(*mockAttributeReader, ReadDataAndLen(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(bytesValue, bytesValue + sizeof(dataValue)), 
                        SetArgReferee<3>(sizeof(int32_t)),
                        Return(true)));

    UpdatableVarNumAttributeMergerForTest<int32_t> merger;
    EXPECT_CALL(merger, CreateSegmentReader(_))
        .WillOnce(Return(attributeReader));

    EXPECT_CALL(merger, CreatePatchReaderForSegment(_))
        .WillOnce(Return(patchReader));

    string segMergeInfosString = "1()";
    ReclaimMapPtr reclaimMap =
        AttributeMergerTestUtil::CreateReclaimMap(segMergeInfosString);

    merger.Init(AttributeMergerTestUtil::CreateAttributeConfig<int32_t>(true));

    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,3,0");
    Version version(0);
    version.AddSegment(0);
    SegmentDirectoryPtr segDir(new SegmentDirectory(mTestDataPath, version));
    segDir->Init(false);
    merger.BeginMerge(segDir);

    SegmentMergeInfos segMergeInfos =
        AttributeMergerTestUtil::CreateSegmentMergeInfos(segMergeInfosString);

    MergerResource resource;
    resource.targetSegmentCount = 1;
    resource.reclaimMap = reclaimMap;
    OutputSegmentMergeInfo segInfo;
    segInfo.targetSegmentIndex = 0;
    segInfo.path = "";
    segInfo.directory = DirectoryCreator::Create(segInfo.path);
    merger.Merge(resource, segMergeInfos, {segInfo});

    // check
    const vector<string> &dataVec = merger.GetActualDataVec();
    const vector<string> &offsetVec = merger.GetActualOffsetVec();
    EXPECT_EQ((size_t)1, dataVec.size());
    EXPECT_EQ(1, *(int32_t*)(dataVec[0].data()));
    EXPECT_EQ((size_t)1, offsetVec.size());
    EXPECT_EQ((uint64_t)0, *(uint64_t*)(offsetVec[0].data()));
    
    vector<AttributeSegmentReaderPtr> attributeReaderVec;
    vector<VarNumAttributeTestUtil<int32_t>::PatchReaderPtr> patchReaderVec;
}

void UpdatableVarNumAttributeMergerTest::TestMergeWithoutPatch()
{
    // this case is the abstract version of TestSimpleProcess
    CheckMerge<int32_t>(":1:", "1()", "0:1");
    CheckMerge<uint64_t>(":1:|:2:|:NULL:|:4,5:", "4()", "0:1|10:2|20:NULL|22:4,5");

    // with deletion in heap
    CheckMerge<uint64_t>(":1:|:2:|:NULL:|:4,5:", "4(1)", "0:1|10:NULL|12:4,5");
}

void UpdatableVarNumAttributeMergerTest::TestMergeWithPatch()
{
    CheckMerge<int32_t>(":1:2", "1()", "0:2");
    CheckMerge<int32_t>(":1:|:NULL:2", "2()", "0:1|6:2");
    CheckMerge<int32_t>(":1:NULL|:2:22,33|:NULL:", "3()", "0:NULL|2:22,33|12:NULL");
}

void UpdatableVarNumAttributeMergerTest::TestMergeWithEmptyHeap()
{
    CheckMerge<int32_t>("", "0()", "");
    CheckMerge<int32_t>(":1:", "1(0)", "");
}

void UpdatableVarNumAttributeMergerTest::TestCreatePatchReaderWithNoPatches()
{
    UpdatableVarNumAttributeMergerForTest<int32_t> merger;
    EXPECT_CALL(merger, CreatePatchReaderForSegment(0))
        .WillOnce(Invoke(&merger,
                         &UpdatableVarNumAttributeMergerForTest<int32_t>::RealCreatePatchReaderForSegment));

    MockVarNumAttributePatchReader<int32_t> *mockPatchReader = 
        new MockVarNumAttributePatchReader<int32_t>;
    AttributePatchReaderPtr patchReader(mockPatchReader);
    EXPECT_CALL(*mockPatchReader, AddPatchFile(_, _))
        .Times(0);
    EXPECT_CALL(merger, CreatePatchReader())
        .WillOnce(Return(patchReader));

    merger.Init(AttributeMergerTestUtil::CreateAttributeConfig<int32_t>(true));

    MockSegmentDirectory *mockSegDir = new MockSegmentDirectory(mTestDataPath, Version());
    SegmentDirectoryPtr segDir(mockSegDir);
    segDir->Init(false);
    vector<pair<string, segmentid_t> > patches;
    merger.BeginMerge(segDir);
    
    AttributePatchReaderPtr actualPatchReader = 
        merger.CreatePatchReaderForSegment(0);
    EXPECT_TRUE(patchReader == actualPatchReader);
}


void UpdatableVarNumAttributeMergerTest::TestMergePatchesLegacy()
{
    // TODO: delete below when not support old format version
    IndexPartitionOptions defaultOptions;
    if (defaultOptions.GetBuildConfig().enablePackageFile)
    {
        // not run case when default enablePackageFile is true
        return;
    }

    {
        SCOPED_TRACE("no segment need merge patch");
        CheckMergePatches("0", "0", "", "");
    }
    {
        SCOPED_TRACE("one segment need merge, but no patch");
        CheckMergePatches("0,1,2,3,4", "0,2", "1:", "");
    }
    {
        SCOPED_TRACE("segment 1 need merge patches, it has patches 3,4, but 3,4 > max(all merge segment");
        CheckMergePatches("0,1,2,3,4", "0,2", "1:3,4", "");
    }
    {
        SCOPED_TRACE("segment 1 need merge patches, it has patches 2,3,4 and segment 2 participate in merge");
        CheckMergePatches("0,1,2,3,4", "0,2", "1:2,3,4", "1:2,3,4");
    }
}


IE_NAMESPACE_END(index);

