#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/test/partition_data_maker.h"
#include "attribute_merger_test_util.h"
#include "updatable_var_num_attribute_merger_test_util.h"
#include "updatable_uniq_encoded_var_num_attribute_merger_unittest.h"

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
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UpdatableUniqEncodedVarNumAttributeMergerTest);

template <typename T>
class UpdatableUniqEncodedVarNumAttributeMergerForTest : public UniqEncodedVarNumAttributeMerger<T>
{
public:
    UpdatableUniqEncodedVarNumAttributeMergerForTest()
    : UniqEncodedVarNumAttributeMerger<T>(true)
    {}
    ~UpdatableUniqEncodedVarNumAttributeMergerForTest()
    {}

public:
    MOCK_METHOD1_T(CreateSegmentReader, 
                   AttributeSegmentReaderPtr(const SegmentMergeInfo& segMergeInfo));
    MOCK_METHOD1_T(CreatePatchReaderForSegment, 
                   AttributePatchReaderPtr(segmentid_t segmentId));
    MOCK_METHOD1_T(CreatePatchReader,
                   AttributePatchReaderPtr(segmentid_t segmentId));
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
UpdatableUniqEncodedVarNumAttributeMergerTest::UpdatableUniqEncodedVarNumAttributeMergerTest()
{
}

UpdatableUniqEncodedVarNumAttributeMergerTest::~UpdatableUniqEncodedVarNumAttributeMergerTest()
{
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::CaseSetUp()
{
    mTestDataPath = GET_TEST_DATA_PATH();
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::CaseTearDown()
{
}

/*
 * Format: 
 * dataAndPatchString: REFER TO CreateAttributeReadersAndPatchReaders 
 */
template<typename T>
AttributeMergerPtr UpdatableUniqEncodedVarNumAttributeMergerTest::CreateAttributeMerger(
        const string &dataAndPatchString)
{
    vector<AttributeSegmentReaderPtr> attributeReaderVec;
    vector<typename VarNumAttributeTestUtil<T>::PatchReaderPtr> patchReaderVec;
    uint32_t segNum = VarNumAttributeTestUtil<T>::CreateAttributeReadersAndPatchReaders(
            dataAndPatchString, 
            attributeReaderVec, patchReaderVec);

    NiceMock<UpdatableUniqEncodedVarNumAttributeMergerForTest<T> > *merger =
        new NiceMock<UpdatableUniqEncodedVarNumAttributeMergerForTest<T> >;
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
void UpdatableUniqEncodedVarNumAttributeMergerTest::CheckDataAndOffset(
        const AttributeMergerPtr &mergerPtr,
        const string &expectDataAndOffsetString)
{
    NiceMock<UpdatableUniqEncodedVarNumAttributeMergerForTest<T> > *merger = 
        (NiceMock<UpdatableUniqEncodedVarNumAttributeMergerForTest<T> > *)(mergerPtr.get());
    const vector<string> &dataVec = merger->GetActualDataVec();
    const vector<string> &offsetVec = merger->GetActualOffsetVec();
    vector<string> expectDataVec;
    vector<string> expectOffsetVec;
    VarNumAttributeTestUtil<T>::CreateExpectMergedDataAndOffset(
            expectDataAndOffsetString, expectDataVec, expectOffsetVec);
    EXPECT_EQ(expectOffsetVec.size(), offsetVec.size());
    EXPECT_EQ(expectOffsetVec, offsetVec)
        << "HINT    : [" << expectDataAndOffsetString << "]";
    EXPECT_EQ(expectDataVec, dataVec) 
        << "HINT    : [" << expectDataAndOffsetString << "]";
}

/*
 * Format:
 * dataAndPatchString       : REFER TO CreateAttributeReadersAndPatchReaders
 * segMergeInfosString      : REFER TO CreateSegmentMergeInfos
 * expectDataAndOffsetString: REFER TO CreateExpectMergedDataAndOffset
 */
template<typename T>
void UpdatableUniqEncodedVarNumAttributeMergerTest::CheckMerge(
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
    SegmentDirectoryPtr segDir(
            new SegmentDirectory(mTestDataPath, version));
    segDir->Init(false);
    mergerPtr->BeginMerge(segDir);
    mergerPtr->Merge("", segMergeInfos, reclaimMap);

    CheckDataAndOffset<T>(mergerPtr, expectDataAndOffsetString);
}

//////////////////////////////////////////////////////////////////////
void UpdatableUniqEncodedVarNumAttributeMergerTest::TestSimpleProcess()
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
    EXPECT_CALL(*mockAttributeReader, ReadDataAndLen(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(bytesValue,
                                bytesValue + sizeof(dataValue)),
                        SetArgReferee<3>(sizeof(int32_t)), 
                        Return(true)));

    EXPECT_CALL(*mockAttributeReader, GetOffset(_))
        .Times(2)
        .WillRepeatedly(Return(0));

    UpdatableUniqEncodedVarNumAttributeMergerForTest<int32_t> merger;
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
    typedef std::tr1::shared_ptr<VarNumAttributePatchReader<int32_t> > PatchReaderPtr;
    vector<PatchReaderPtr> patchReaderVec;
}

/*
 * dataAndPatchString       : "offset1:v11,...:p11,...|...;..."
 * segMergeInfosString      : "docCount1(delId11,delId12,...);..."
 * expectDataAndOffsetString: "offset1[,count]:v11,...|..."
 */
void UpdatableUniqEncodedVarNumAttributeMergerTest::TestMergeWithoutPatch()
{
    //this case is the abstract version of TestSimpleProcess
    CheckMerge<int32_t>("0:1:", "1()", "0:1");

    {
        SCOPED_TRACE("Normal");
        CheckMerge<uint64_t>("0:1:|10:2:|20:NULL:|22:4,5:", "4()",
                             "0:1|10:2|20:NULL|22:4,5");
        CheckMerge<int8_t>("0:0:|3:2,21:|7:1:|10:3:", "4()",
                           "0:0|3:2,21|7:1|10:3");
        CheckMerge<int8_t>("0:0:|3:2,21:|7:NULL:|9:3:", "4()",
                           "0:0|3:2,21|7:NULL|9:3");
    }
    {
        SCOPED_TRACE("with deletion in heap");
        CheckMerge<uint64_t>("0:1:|10:2:|20:NULL:|22:4,5:", "4(1)",
                             "0:1|10:NULL|12:4,5");
    }
    {
        SCOPED_TRACE("multi segment");
        CheckMerge<int8_t>("0:0:|3:2,21:|7:NULL:|9:3:; 0:11,22:", "4(); 1()",
                           "0:0|3:2,21|7:NULL|9:3|12:11,22");
    }
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::TestUniqEncodeMergeWithoutPatch()
{
    {
        SCOPED_TRACE("no seek back, offset is crossing");
        CheckMerge<int8_t>("0:0:|6:2:|3:1:|9:3:", "4()",
                           "0:0|6:1|3:2|9:3");
    }
    {
        SCOPED_TRACE("adjacent value NULL and value non-NULL");
        CheckMerge<int8_t>("0:NULL:|2:2:|5:1:|8:3:", "4()",
                           "0:NULL|2:2|5:1|8:3");
        CheckMerge<int8_t>("0:2:|2:NULL:|5:1:|8:3:", "4()",
                           "0:2|3:NULL|5:1|8:3");
        CheckMerge<int8_t>("0:2:|3:NULL:|3:NULL:|5:3:", "4()",
                           "0:2|3:NULL|3:|5:3");
        CheckMerge<int8_t>("0:NULL:|2:2:|0:NULL:|5:3:", "4()",
                           "0:NULL|2:2|0:|5:3");
    }
    {
        SCOPED_TRACE("uniq encode");
        CheckMerge<int8_t>("0:1:|0:1:|0:1:|0:1:", "4()",
                           "0:1|0:|0:|0:");
    }
    {
        SCOPED_TRACE("multi segment");
        CheckMerge<int8_t>("0:1:|3:2:; 0:1:|3:2:", "2(); 2()",
                           "0:1|3:2|0:|3:");
        CheckMerge<int8_t>("0:1:|3:2:; 0:1:|3:2:; 0:1:", "2(); 2(); 1(0)",
                           "0:1|3:2|0:|3:");
    }
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::TestMergeWithPatch()
{
    CheckMerge<int32_t>("0:1:2", "1()", "0:2");
    CheckMerge<int32_t>("0:1:|6:NULL:2", "2()", "6:2|0:1");
    CheckMerge<int32_t>("0:1:NULL|6:2:22,33|16:3:", "3()",
                        "0:NULL|2:22,33|12:3");
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::TestUniqEncodeMergeWithPatch()
{
    {
        SCOPED_TRACE("one segment");
        CheckMerge<int32_t>("0:1:2", "1()", "0:2");
        CheckMerge<int32_t>("0:1:2|6:NULL:2", "2()",
                            "0:2|0:");
        CheckMerge<int32_t>("0:1:NULL|6:2:22,33|16:NULL:", "3()",
                            "0:NULL|2:22,33|0:");
    }
    {
        SCOPED_TRACE("two segemnt and data only from patch");
        CheckMerge<int8_t>("0:1:10|3:2:20; 0:1:10|3:2:20", "2(); 2()",
                           "0:10|3:20|0:|3:");
    }
    {
        SCOPED_TRACE("two segment only first segment has patch");
        CheckMerge<int8_t>("0:10:|3:20:; 0:1:10|3:2:20", "2(); 2()",
                           "0:10|3:20|0:|3:");
    }
    {
        SCOPED_TRACE("two segment only second segment has patch");
        CheckMerge<int8_t>("0:1:10|3:2:20; 0:10:|3:20:",
                           "2(); 2()",
                           "0:10|3:20|0:|3:");
    }
    {
        SCOPED_TRACE("two segment has two patch");
        CheckMerge<int8_t>("0:1:10|3:2:; 0:1:|3:2:20",
                           "2(); 2()",
                           "0:10|6:20|9:2|3:1");
    }
    {
        SCOPED_TRACE("three segment with delete has two patch");
        CheckMerge<int8_t>("0:1:10|3:2:20|6:2:20|9:2:20; 0:1:|3:2:21|6:NULL:20; 0:1:10|0:1:10",
                           "4(1,3); 3(); 2()",
                           "0:10|3:20| 9:21|6:1|3:| 0:|0:");
        CheckMerge<int8_t>("0:1:10,20,30|3:2:20|6:2:20|9:2:20; 0:1:|3:2:21|6:NULL:20; 0:1:10,20,30|0:1:10,20,30",
                           "4(1,3); 3(); 2()",
                           "0:10,20,30|5:20| 11:21|8:1|5:| 0:|0:");
        CheckMerge<int8_t>("0:1:10,20,30|3:2:NULL|6:2:NULL|9:2:20; 0:1:|3:2:21|6:NULL:NULL; 0:1:10,20,30|3:NULL:",
                           "4(1,3); 3(); 2()",
                           "0:10,20,30|5:NULL| 10:21|7:1|5:| 0:|5:");
    }
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::TestMergeWithEmptyHeap()
{
    CheckMerge<int32_t>("", "0()", "");
    CheckMerge<int32_t>(":1:", "1(0)", "");
}

void UpdatableUniqEncodedVarNumAttributeMergerTest::TestFillOffsetToFirstOldDocIdMapVec()
{
    typedef UniqEncodedVarNumAttributeMerger<int32_t>::DocIdSet DocIdSet;
    typedef UniqEncodedVarNumAttributeMerger<int32_t>::OffsetPair OffsetPair;
    typedef UniqEncodedVarNumAttributeMerger<int32_t>::SegmentOffsetMap SegmentOffsetMap;
    
    vector<AttributeSegmentReaderPtr> attributeReaderVec;
    vector<VarNumAttributeTestUtil<int32_t>::PatchReaderPtr> patchReaderVec;
    VarNumAttributeTestUtil<int32_t>::CreateAttributeReadersAndPatchReaders(
            "0:1:|6:2:|12:3:|18:4:;0:11:|6:22:|12:33:|18:44:;0:1:|0:1:",
            attributeReaderVec, patchReaderVec);
    SegmentMergeInfos segMergeInfos =
        AttributeMergerTestUtil::CreateSegmentMergeInfos("4(0);4(1,3);2()");
    ReclaimMapPtr reclaimMap =
        AttributeMergerTestUtil::CreateReclaimMap("4(0);4(1,3);2()");
    DocIdSet patchedGlobalDocIdSet;
    patchedGlobalDocIdSet.insert(0);
    patchedGlobalDocIdSet.insert(2);
    std::vector<SegmentOffsetMap> firstValidOffsetVec;

    UniqEncodedVarNumAttributeMerger<int32_t> merger(true);

    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        SegmentOffsetMap segmentOffsetMap;
        merger.ConstuctSegmentOffsetMap(
                segMergeInfos[i], reclaimMap, patchedGlobalDocIdSet,
                attributeReaderVec[i], segmentOffsetMap);
        firstValidOffsetVec.push_back(segmentOffsetMap);
    }

#define FIND(firstValidOffsetVec, item)         \
    find(firstValidOffsetVec.begin(), firstValidOffsetVec.end(), OffsetPair(item)) != firstValidOffsetVec.end()

    EXPECT_EQ((size_t)3, firstValidOffsetVec.size());
    EXPECT_EQ((size_t)2, firstValidOffsetVec[0].size());
    EXPECT_EQ((size_t)2, firstValidOffsetVec[1].size());
    EXPECT_EQ((size_t)1, firstValidOffsetVec[2].size());
    EXPECT_FALSE(FIND(firstValidOffsetVec[0], 0)); // patch
    EXPECT_TRUE(FIND(firstValidOffsetVec[0], 6));
    EXPECT_FALSE(FIND(firstValidOffsetVec[0], 12)); // patch
    EXPECT_TRUE(FIND(firstValidOffsetVec[0], 18));
    EXPECT_TRUE(FIND(firstValidOffsetVec[1], 0));
    EXPECT_FALSE(FIND(firstValidOffsetVec[1], 6)); // del
    EXPECT_TRUE(FIND(firstValidOffsetVec[1], 12));
    EXPECT_FALSE(FIND(firstValidOffsetVec[1], 18)); // del
    EXPECT_TRUE(FIND(firstValidOffsetVec[2], 0));
}

IE_NAMESPACE_END(index);

