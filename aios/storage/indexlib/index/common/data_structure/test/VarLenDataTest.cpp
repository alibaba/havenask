#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"
#include "indexlib/index/common/data_structure/VarLenDataDumper.h"
#include "indexlib/index/common/data_structure/VarLenDataMerger.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"
#include "indexlib/index/test/FakeDocMapper.h"
#include "unittest/unittest.h"

using std::string;
using std::vector;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class VarLenDataTest : public TESTBASE
{
public:
    VarLenDataTest() = default;
    ~VarLenDataTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    /* para_str= adaptiveOffset[1000]|equal|uniq|appendLen|guardOffset|compressor=zlib[2048] */
    VarLenDataParam CreateParam(const std::string& para_str);
    void InnerTest(const std::string& para_str, bool useBlockCache);

    void InnerTestBuildAndRead(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                               const VarLenDataParam& param, size_t segCount, size_t repeatDocCountPerSegment);

    void InnerTestMergeData(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                            const VarLenDataParam& param, size_t segCount, size_t repeatDocCountPerSegment);

    std::pair<Status, std::vector<VarLenDataMerger::InputData>>
    MakeInputDatas(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const VarLenDataParam& param,
                   size_t segCount, size_t repeatDocCountPerSegment,
                   IIndexMerger::SegmentMergeInfos& segmentMergeInfos);

    std::pair<Status, std::vector<VarLenDataMerger::OutputData>>
    MakeOutputDatas(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const VarLenDataParam& param,
                    segmentid_t segId, IIndexMerger::SegmentMergeInfos& segmentMergeInfos);

    std::shared_ptr<DocMapper> CreateDocMapper(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos);
    void ResetRootDirectory(indexlib::file_system::LoadConfigList& loadConfigList);

private:
    std::vector<std::string> _data;
    autil::mem_pool::Pool _pool;
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
};

namespace {
static std::string docMapperName = "Fake_Doc_Map";
} // namespace

void VarLenDataTest::setUp()
{
    _data.push_back(std::string("test_var_len_data"));
    _data.push_back(std::string("alibaba_group"));
    _data.push_back(std::string("storage_test"));
    _data.push_back(std::string("hello_world"));

    _rootPath = GET_TEMP_DATA_PATH() + "/";
    indexlib::file_system::LoadConfigList loadConfigList;
    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");
    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_SUMMARY__");
    loadConfigList.PushFront(loadConfig);
    ResetRootDirectory(loadConfigList);
}

void VarLenDataTest::tearDown() { _data.clear(); }

void VarLenDataTest::ResetRootDirectory(indexlib::file_system::LoadConfigList& loadConfigList)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;
    fsOptions.loadConfigList = loadConfigList;

    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto directory = indexlib::file_system::Directory::Get(fs);
    assert(directory);
    _rootDir = directory->GetIDirectory();
}

void VarLenDataTest::InnerTest(const std::string& para_str, bool useBlockCache)
{
    tearDown();
    setUp();

    if (useBlockCache) {
        LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
        ResetRootDirectory(loadConfigList);
    }

    VarLenDataParam param = CreateParam(para_str);
    size_t segCount = 5;
    size_t repeatDocCountPerSegment = 2;
    auto directory = _rootDir;
    InnerTestBuildAndRead(directory, param, segCount, repeatDocCountPerSegment);
    InnerTestMergeData(directory, param, segCount, repeatDocCountPerSegment);
}

void VarLenDataTest::InnerTestBuildAndRead(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                           const VarLenDataParam& param, size_t segCount,
                                           size_t repeatDocCountPerSegment)
{
    for (size_t segId = 0; segId < segCount; segId++) {
        VarLenDataAccessor accessor;
        accessor.Init(&_pool, param.dataItemUniqEncode);
        uint32_t docCount = 0;
        for (size_t i = 0; i < repeatDocCountPerSegment; i++) {
            for (auto data : _data) {
                std::string field = data + StringUtil::toString(segId);
                accessor.AppendValue(StringView(field));
                docCount++;
            }
        }
        auto [status, segDir] = directory
                                    ->MakeDirectory(std::string("segment_") + StringUtil::toString(segId),
                                                    indexlib::file_system::DirectoryOption())
                                    .StatusWith();
        ASSERT_TRUE(status.IsOK());
        VarLenDataDumper dumper;
        dumper.Init(&accessor, param);
        ASSERT_TRUE(dumper.Dump(segDir, "offset", "data", nullptr, nullptr, &_pool).IsOK());

        auto CheckReader = [this](const VarLenDataParam& param, uint32_t docCount,
                                  const std::shared_ptr<indexlib::file_system::IDirectory>& segDir, size_t segId,
                                  bool isOnline) {
            VarLenDataReader reader(param, isOnline);
            ASSERT_TRUE(reader.Init(docCount, segDir, "offset", "data").IsOK());
            for (docid_t docId = 0; docId < (docid_t)docCount; docId++) {
                StringView value;
                if (param.dataItemUniqEncode && !param.appendDataItemLength) {
                    auto [status, ret] = reader.GetValue(docId, value, &_pool);
                    ASSERT_FALSE(ret);
                } else {
                    auto [status, ret] = reader.GetValue(docId, value, &_pool);
                    ASSERT_TRUE(status.IsOK() && ret);
                    size_t idx = docId % _data.size();
                    std::string expectField = _data[idx] + StringUtil::toString(segId);
                    StringView expectValue(expectField);
                    ASSERT_EQ(expectValue, value);
                }
            }
        };
        CheckReader(param, docCount, segDir, segId, false);
        CheckReader(param, docCount, segDir, segId, true);
    }
}

void VarLenDataTest::InnerTestMergeData(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                        const VarLenDataParam& param, size_t segCount, size_t repeatDocCountPerSegment)
{
    // prepare merge info|input|output|docmapper
    IIndexMerger::SegmentMergeInfos segmentMergeInfos;
    auto [inStatus, inputDatas] =
        MakeInputDatas(directory, param, segCount, repeatDocCountPerSegment, segmentMergeInfos);
    ASSERT_TRUE(inStatus.IsOK());
    auto [outStatus, outputDatas] = MakeOutputDatas(directory, param, segCount, segmentMergeInfos);
    ASSERT_TRUE(outStatus.IsOK());
    auto docMapper = CreateDocMapper(segmentMergeInfos);

    // do merge
    VarLenDataMerger merger(param);
    merger.Init(segmentMergeInfos, docMapper, inputDatas, outputDatas);

    if (param.dataItemUniqEncode && !param.appendDataItemLength) {
        ASSERT_FALSE(merger.Merge().IsOK());
        auto outputs = merger.GetOutputDatas();
        for (auto output : outputs) {
            // avoid case assert false
            ASSERT_TRUE(output.dataWriter->Close().IsOK());
        }
        return;
    }

    ASSERT_TRUE(merger.Merge().IsOK());

    // check merge result
    size_t totalDocCount = segCount * repeatDocCountPerSegment * _data.size();
    auto [status, outputDir] =
        directory->GetDirectory(std::string("segment_") + StringUtil::toString(segCount)).StatusWith();
    ASSERT_TRUE(status.IsOK());
    VarLenDataReader reader(param, true);
    ASSERT_TRUE(reader.Init(totalDocCount, outputDir, "offset", "data").IsOK());
    for (docid_t docId = 0; docId < (docid_t)totalDocCount; docId++) {
        StringView value;
        auto [status, ret] = reader.GetValue(docId, value, &_pool);
        ASSERT_TRUE(status.IsOK() && ret);
        size_t idx = docId % _data.size();
        size_t expectSegId = docId / (repeatDocCountPerSegment * _data.size());
        std::string expectField = _data[idx] + StringUtil::toString(expectSegId);
        StringView expectValue(expectField);
        ASSERT_EQ(expectValue, value);
    }
}

/* para_str= adaptiveOffset[1000]|equal|uniq|appendLen|noGuardOffset|compressor=zlib[2048] */
VarLenDataParam VarLenDataTest::CreateParam(const std::string& para_str)
{
    VarLenDataParam param;
    std::vector<std::string> tmpVec;
    StringUtil::fromString(para_str, tmpVec, "|");
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
            std::string value = str.substr(14);
            if (value[0] == '[') {
                assert(value[value.length() - 1] == ']');
                StringUtil::fromString(value.substr(1, value.length() - 2), param.offsetThreshold);
            }
        }
        if (str.find("compressor=") == 0) {
            std::string value = str.substr(11);
            size_t pos = value.find("[");
            if (pos == std::string::npos) {
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

std::pair<Status, std::vector<VarLenDataMerger::InputData>>
VarLenDataTest::MakeInputDatas(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                               const VarLenDataParam& param, size_t segCount, size_t repeatDocCountPerSegment,
                               IIndexMerger::SegmentMergeInfos& segmentMergeInfos)
{
    std::vector<VarLenDataMerger::InputData> inputDatas;
    docid_t totalDocCount = 0;
    uint32_t docCount = repeatDocCountPerSegment * _data.size();
    for (size_t i = 0; i < segCount; i++) {
        // create new segment
        auto segment = std::make_shared<framework::FakeSegment>(framework::Segment::SegmentStatus::ST_BUILT);
        auto& segmentMeta = segment->_segmentMeta;
        auto& segmentInfo = segment->GetSegmentInfo();
        segmentMeta.segmentId = i;
        segmentInfo->docCount = docCount;

        // make source segment info
        IIndexMerger::SourceSegment srcSegment;
        srcSegment.baseDocid = totalDocCount;
        srcSegment.segment = segment;
        segmentMergeInfos.srcSegments.emplace_back(srcSegment);
        totalDocCount += docCount;

        // make input data
        VarLenDataMerger::InputData input;
        input.dataReader.reset(new VarLenDataReader(param, false));
        auto [status, segDir] = directory->GetDirectory(std::string("segment_") + StringUtil::toString(i)).StatusWith();
        if (!status.IsOK()) {
            return std::make_pair(status, inputDatas);
        }
        assert(status.IsOK());
        status = input.dataReader->Init(docCount, segDir, "offset", "data");
        if (!status.IsOK()) {
            return std::make_pair(status, inputDatas);
        }
        inputDatas.push_back(input);
    }
    return std::make_pair(Status::OK(), inputDatas);
}

std::shared_ptr<DocMapper> VarLenDataTest::CreateDocMapper(const index::IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    auto docMapper =
        std::make_shared<FakeDocMapper>(/*name*/ docMapperName, /*type*/ index::DocMapper::GetDocMapperType());

    std::vector<SrcSegmentInfo> srcSegmentInfos;
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        uint32_t segDocCount = srcSegment.segment->GetSegmentInfo()->docCount;
        auto segId = srcSegment.segment->GetSegmentId();
        auto baseDocId = srcSegment.baseDocid;
        std::set<docid_t> deletedDocs;
        SrcSegmentInfo srcSegmentInfo(segId, segDocCount, baseDocId, deletedDocs);
        srcSegmentInfos.emplace_back(srcSegmentInfo);
    }

    docMapper->Init(segMergeInfos.targetSegments[0]->segmentId, srcSegmentInfos, nullptr);
    for (auto& targetSegment : segMergeInfos.targetSegments) {
        targetSegment->segmentInfo->docCount = docMapper->GetTargetSegmentDocCount(targetSegment->segmentId);
    }
    return docMapper;
}

std::pair<Status, std::vector<VarLenDataMerger::OutputData>>
VarLenDataTest::MakeOutputDatas(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                const VarLenDataParam& param, segmentid_t segId,
                                IIndexMerger::SegmentMergeInfos& segmentMergeInfos)
{
    std::vector<VarLenDataMerger::OutputData> outputDatas;
    auto segmentMeta = std::make_shared<framework::SegmentMeta>();
    segmentMeta->segmentId = segId;
    segmentMergeInfos.targetSegments.emplace_back(segmentMeta);

    auto [status, outputDir] = directory
                                   ->MakeDirectory(std::string("segment_") + StringUtil::toString(segId),
                                                   indexlib::file_system::DirectoryOption())
                                   .StatusWith();
    if (!status.IsOK()) {
        return std::make_pair(status, outputDatas);
    }
    VarLenDataMerger::OutputData output;
    output.targetSegmentId = segId;
    output.dataWriter.reset(new VarLenDataWriter(&_pool));
    status = output.dataWriter->Init(outputDir, "offset", "data", nullptr, param);
    if (!status.IsOK()) {
        return std::make_pair(status, outputDatas);
    }
    outputDatas.push_back(output);
    return std::make_pair(Status::OK(), outputDatas);
}

TEST_F(VarLenDataTest, TestSimpleProcess)
{
    InnerTest("", false);
    InnerTest("equal|appendLen", false);
    InnerTest("adaptiveOffset[20]|equal", true);
    InnerTest("adaptiveOffset|equal|uniq|appendLen", false);
    InnerTest("adaptiveOffset|equal|uniq|appendLen|compressor=zstd", true);
    InnerTest("uniq", true);
}

TEST_F(VarLenDataTest, TestBatchGetValueBadParam)
{
    std::string para_str = "adaptiveOffset|equal|uniq|appendLen|compressor=zstd";
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    ResetRootDirectory(loadConfigList);
    VarLenDataParam param = CreateParam(para_str);
    size_t repeatDocCountPerSegment = 2;
    auto directory = _rootDir;
    VarLenDataAccessor accessor;
    accessor.Init(&_pool, param.dataItemUniqEncode);
    uint32_t docCount = 0;
    uint32_t segId = 0;
    for (size_t i = 0; i < repeatDocCountPerSegment; i++) {
        for (auto data : _data) {
            std::string field = data + StringUtil::toString(segId);
            accessor.AppendValue(StringView(field));
            docCount++;
        }
    }
    auto [status, segDir] = directory
                                ->MakeDirectory(std::string("segment_") + StringUtil::toString(segId),
                                                indexlib::file_system::DirectoryOption())
                                .StatusWith();
    ASSERT_TRUE(status.IsOK());
    VarLenDataDumper dumper;
    dumper.Init(&accessor, param);
    ASSERT_TRUE(dumper.Dump(segDir, "offset", "data", nullptr, nullptr, &_pool).IsOK());

    VarLenDataReader reader(param, true);
    ASSERT_TRUE(reader.Init(docCount, segDir, "offset", "data").IsOK());
    ASSERT_TRUE(docCount >= 2);
    autil::mem_pool::Pool pool;
    // test docid is not increasing
    {
        std::vector<docid_t> docIds {1, 0};
        std::vector<StringView> values;
        indexlib::index::ErrorCodeVec expectedRet(docIds.size(), indexlib::index::ErrorCode::Runtime);
        auto ret =
            future_lite::coro::syncAwait(reader.GetValue(docIds, &pool, indexlib::file_system::ReadOption(), &values));
        ASSERT_EQ(expectedRet, ret);
    }
    // test docid is out of bounds
    {
        std::vector<docid_t> docIds {0, (docid_t)(docCount + 1)};
        std::vector<StringView> values;
        indexlib::index::ErrorCodeVec expectedRet(docIds.size(), indexlib::index::ErrorCode::Runtime);
        auto ret =
            future_lite::coro::syncAwait(reader.GetValue(docIds, &pool, indexlib::file_system::ReadOption(), &values));
        ASSERT_EQ(expectedRet, ret);
    }
}

} // namespace indexlibv2::index
