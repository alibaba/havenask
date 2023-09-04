#include "indexlib/common_define.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/data_structure/var_len_data_accessor.h"
#include "indexlib/index/data_structure/var_len_data_dumper.h"
#include "indexlib/index/data_structure/var_len_data_merger.h"
#include "indexlib/index/data_structure/var_len_data_reader.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using std::string;
using std::vector;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {

class VarLenDataTest : public INDEXLIB_TESTBASE
{
public:
    VarLenDataTest();
    ~VarLenDataTest();

    DECLARE_CLASS_NAME(VarLenDataTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBatchGetValueBadParam();

private:
    /* paramStr= adaptiveOffset[1000]|equal|uniq|appendLen|guardOffset|compressor=zlib[2048] */
    VarLenDataParam CreateParam(const string& paramStr);
    void InnerTest(const string& paramStr, bool useBlockCache, bool sortMerge);

    void InnerTestBuildAndRead(const file_system::DirectoryPtr& directory, const VarLenDataParam& param,
                               size_t segCount, size_t repeatDocCountPerSegment);

    void InnerTestMergeData(const file_system::DirectoryPtr& directory, const VarLenDataParam& param, size_t segCount,
                            size_t repeatDocCountPerSegment, bool sortMerge);

    vector<VarLenDataMerger::InputData> MakeInputDatas(const file_system::DirectoryPtr& directory,
                                                       const VarLenDataParam& param, size_t segCount,
                                                       size_t repeatDocCountPerSegment);

    vector<VarLenDataMerger::OutputData> MakeOutputDatas(const file_system::DirectoryPtr& directory,
                                                         const VarLenDataParam& param, segmentid_t segId);

    MergerResource MakeMergerResource(size_t segCount, size_t repeatDocCountPerSegment, bool sortMerge);

private:
    vector<string> mData;
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarLenDataTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(VarLenDataTest, TestBatchGetValueBadParam);
IE_LOG_SETUP(index, VarLenDataTest);

VarLenDataTest::VarLenDataTest() {}

VarLenDataTest::~VarLenDataTest() {}

void VarLenDataTest::CaseSetUp()
{
    mData.push_back(string("test_var_len_data"));
    mData.push_back(string("alibaba_group"));
    mData.push_back(string("storage_test"));
    mData.push_back(string("hello_world"));
}

void VarLenDataTest::CaseTearDown() { mData.clear(); }

void VarLenDataTest::TestSimpleProcess()
{
    InnerTest("", false, false);
    InnerTest("equal|appendLen", false, true);
    InnerTest("adaptiveOffset[20]|equal", true, false);
    InnerTest("adaptiveOffset|equal|uniq|appendLen", false, true);
    InnerTest("adaptiveOffset|equal|uniq|appendLen|compressor=zstd", true, false);
    InnerTest("uniq", true, true);
}

void VarLenDataTest::TestBatchGetValueBadParam()
{
    string paramStr = "adaptiveOffset|equal|uniq|appendLen|compressor=zstd";
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    RESET_FILE_SYSTEM(loadConfigList, false);
    VarLenDataParam param = CreateParam(paramStr);
    size_t repeatDocCountPerSegment = 2;
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, param.dataItemUniqEncode);
    uint32_t docCount = 0;
    uint32_t segId = 0;
    for (size_t i = 0; i < repeatDocCountPerSegment; i++) {
        for (auto data : mData) {
            string field = data + StringUtil::toString(segId);
            accessor.AppendValue(StringView(field));
            docCount++;
        }
    }
    file_system::DirectoryPtr segDir = directory->MakeDirectory(string("segment_") + StringUtil::toString(segId));
    VarLenDataDumper dumper;
    dumper.Init(&accessor, param);
    dumper.Dump(segDir, "offset", "data", nullptr, nullptr, &mPool);

    VarLenDataReader reader(param, true);
    reader.Init(docCount, segDir, "offset", "data");
    ASSERT_TRUE(docCount >= 2);
    autil::mem_pool::Pool pool;
    // test docid is not increasing
    {
        std::vector<docid_t> docIds {1, 0};
        vector<StringView> values;
        index::ErrorCodeVec expectedRet(docIds.size(), index::ErrorCode::Runtime);
        auto ret = future_lite::coro::syncAwait(reader.GetValue(docIds, &pool, file_system::ReadOption(), &values));
        ASSERT_EQ(expectedRet, ret);
    }
    // test docid is out of bounds
    {
        std::vector<docid_t> docIds {0, (docid_t)(docCount + 1)};
        vector<StringView> values;
        index::ErrorCodeVec expectedRet(docIds.size(), index::ErrorCode::Runtime);
        auto ret = future_lite::coro::syncAwait(reader.GetValue(docIds, &pool, file_system::ReadOption(), &values));
        ASSERT_EQ(expectedRet, ret);
    }
}

void VarLenDataTest::InnerTest(const string& paramStr, bool useBlockCache, bool sortMerge)
{
    tearDown();
    setUp();

    if (useBlockCache) {
        LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
        RESET_FILE_SYSTEM(loadConfigList, false);
    }

    VarLenDataParam param = CreateParam(paramStr);
    size_t segCount = 5;
    size_t repeatDocCountPerSegment = 2;
    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    InnerTestBuildAndRead(directory, param, segCount, repeatDocCountPerSegment);
    InnerTestMergeData(directory, param, segCount, repeatDocCountPerSegment, sortMerge);
}

void VarLenDataTest::InnerTestBuildAndRead(const file_system::DirectoryPtr& directory, const VarLenDataParam& param,
                                           size_t segCount, size_t repeatDocCountPerSegment)
{
    for (size_t segId = 0; segId < segCount; segId++) {
        VarLenDataAccessor accessor;
        accessor.Init(&mPool, param.dataItemUniqEncode);
        uint32_t docCount = 0;
        for (size_t i = 0; i < repeatDocCountPerSegment; i++) {
            for (auto data : mData) {
                string field = data + StringUtil::toString(segId);
                accessor.AppendValue(StringView(field));
                docCount++;
            }
        }
        file_system::DirectoryPtr segDir = directory->MakeDirectory(string("segment_") + StringUtil::toString(segId));
        VarLenDataDumper dumper;
        dumper.Init(&accessor, param);
        dumper.Dump(segDir, "offset", "data", nullptr, nullptr, &mPool);

        auto CheckReader = [this](const VarLenDataParam& param, uint32_t docCount,
                                  const file_system::DirectoryPtr& segDir, size_t segId, bool isOnline) {
            VarLenDataReader reader(param, isOnline);
            reader.Init(docCount, segDir, "offset", "data");
            for (docid_t docId = 0; docId < (docid_t)docCount; docId++) {
                StringView value;
                if (param.dataItemUniqEncode && !param.appendDataItemLength) {
                    ASSERT_FALSE(reader.GetValue(docId, value, &mPool));
                } else {
                    ASSERT_TRUE(reader.GetValue(docId, value, &mPool));
                    size_t idx = docId % mData.size();
                    string expectField = mData[idx] + StringUtil::toString(segId);
                    StringView expectValue(expectField);
                    ASSERT_EQ(expectValue, value);
                }
            }
        };
        CheckReader(param, docCount, segDir, segId, false);
        CheckReader(param, docCount, segDir, segId, true);
    }
}

void VarLenDataTest::InnerTestMergeData(const file_system::DirectoryPtr& directory, const VarLenDataParam& param,
                                        size_t segCount, size_t repeatDocCountPerSegment, bool sortMerge)
{
    // prepare merge input|output|resource
    auto inputDatas = MakeInputDatas(directory, param, segCount, repeatDocCountPerSegment);
    auto outputDatas = MakeOutputDatas(directory, param, segCount);
    MergerResource resource = MakeMergerResource(segCount, repeatDocCountPerSegment, sortMerge);

    // do merge
    VarLenDataMerger merger(param);
    merger.Init(resource, inputDatas, outputDatas);

    if (param.dataItemUniqEncode && !param.appendDataItemLength) {
        ASSERT_ANY_THROW(merger.Merge());
        auto outputs = merger.GetOutputDatas();
        for (auto output : outputs) {
            // avoid case assert false
            output.dataWriter->Close();
        }
        return;
    }

    merger.Merge();

    // check merge result
    size_t totalDocCount = segCount * repeatDocCountPerSegment * mData.size();
    file_system::DirectoryPtr outputDir =
        directory->GetDirectory(string("segment_") + StringUtil::toString(segCount), false);
    VarLenDataReader reader(param, true);
    reader.Init(totalDocCount, outputDir, "offset", "data");
    for (docid_t docId = 0; docId < (docid_t)totalDocCount; docId++) {
        StringView value;
        ASSERT_TRUE(reader.GetValue(docId, value, &mPool));
        size_t idx = docId % mData.size();
        size_t expectSegId = 0;
        if (!sortMerge) {
            expectSegId = docId / (repeatDocCountPerSegment * mData.size());
        } else {
            expectSegId = (docId % (totalDocCount / 2)) / mData.size();
        }
        string expectField = mData[idx] + StringUtil::toString(expectSegId);
        StringView expectValue(expectField);
        ASSERT_EQ(expectValue, value);
    }
}

/* paramStr= adaptiveOffset[1000]|equal|uniq|appendLen|noGuardOffset|compressor=zlib[2048] */
VarLenDataParam VarLenDataTest::CreateParam(const string& paramStr)
{
    VarLenDataParam param;
    vector<string> tmpVec;
    StringUtil::fromString(paramStr, tmpVec, "|");
    for (auto str : tmpVec) {
        if (str == "equal") {
            param.equalCompressOffset = true;
        }
        if (str == "uniq") {
            param.dataItemUniqEncode = true;
        }
        if (str == "appendLen") {
            param.appendDataItemLength = true;
        }
        if (str == "noGuardOffset") {
            param.disableGuardOffset = true;
        }
        if (str.find("adaptiveOffset") == 0) {
            param.enableAdaptiveOffset = true;
            string value = str.substr(14);
            if (value[0] == '[') {
                assert(value[value.length() - 1] == ']');
                StringUtil::fromString(value.substr(1, value.length() - 2), param.offsetThreshold);
            }
        }
        if (str.find("compressor=") == 0) {
            string value = str.substr(11);
            size_t pos = value.find("[");
            if (pos == string::npos) {
                param.dataCompressorName = value;
            } else {
                param.dataCompressorName = value.substr(0, pos);
                assert(value[value.length() - 1] == ']');
                StringUtil::fromString(value.substr(pos + 1, value.length() - pos - 2), param.dataCompressBufferSize);
            }
        }
    }
    return param;
}

vector<VarLenDataMerger::InputData> VarLenDataTest::MakeInputDatas(const file_system::DirectoryPtr& directory,
                                                                   const VarLenDataParam& param, size_t segCount,
                                                                   size_t repeatDocCountPerSegment)
{
    vector<VarLenDataMerger::InputData> inputDatas;
    docid_t totalDocCount = 0;
    uint32_t docCount = repeatDocCountPerSegment * mData.size();
    for (size_t i = 0; i < segCount; i++) {
        index_base::SegmentMergeInfo mergeInfo;
        mergeInfo.segmentId = i;
        mergeInfo.segmentInfo.docCount = docCount;
        mergeInfo.deletedDocCount = 0;
        mergeInfo.baseDocId = totalDocCount;
        totalDocCount += docCount;

        VarLenDataMerger::InputData input;
        input.segMergeInfo = mergeInfo;
        input.dataReader.reset(new VarLenDataReader(param, false));
        file_system::DirectoryPtr segDir = directory->GetDirectory(string("segment_") + StringUtil::toString(i), false);
        input.dataReader->Init(docCount, segDir, "offset", "data");
        inputDatas.push_back(input);
    }
    return inputDatas;
}

MergerResource VarLenDataTest::MakeMergerResource(size_t segCount, size_t repeatDocCountPerSegment, bool sortMerge)
{
    MergerResource resource;
    vector<uint32_t> segDocCounts(segCount, repeatDocCountPerSegment * mData.size());
    vector<std::set<docid_t>> delDocIdSets;
    delDocIdSets.resize(segCount);
    vector<docid_t> targetBaseDocIds;
    targetBaseDocIds.push_back(0);
    resource.targetSegmentCount = 1;
    if (sortMerge) {
        resource.reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(segDocCounts, delDocIdSets, targetBaseDocIds);
    } else {
        resource.reclaimMap = IndexTestUtil::CreateReclaimMap(segDocCounts, delDocIdSets, false, targetBaseDocIds);
    }
    return resource;
}

vector<VarLenDataMerger::OutputData> VarLenDataTest::MakeOutputDatas(const file_system::DirectoryPtr& directory,
                                                                     const VarLenDataParam& param, segmentid_t segId)
{
    file_system::DirectoryPtr outputDir = directory->MakeDirectory(string("segment_") + StringUtil::toString(segId));
    VarLenDataMerger::OutputData output;
    output.targetSegmentIndex = 0;
    output.dataWriter.reset(new VarLenDataWriter(&mPool));
    output.dataWriter->Init(outputDir, "offset", "data", nullptr, param);
    vector<VarLenDataMerger::OutputData> outputDatas;
    outputDatas.push_back(output);
    return outputDatas;
}
}} // namespace indexlib::index
