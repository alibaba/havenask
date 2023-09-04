#include "indexlib/index/attribute/patch/MultiValueAttributePatchMerger.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/index/attribute/patch/MultiValueAttributeUpdater.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "unittest/unittest.h"

namespace {
static std::string FIELD_NAME_TAG = "price";

std::string GetSegmentName(indexlib::segmentid_t segId)
{
    std::string str("segment_");
    return str + std::to_string(segId) + "_level_0";
}
} // namespace

namespace indexlibv2::index {

template <typename T>
struct TypeParseTraits;

#define REGISTER_PARSE_TYPE(X, Y)                                                                                      \
    template <>                                                                                                        \
    struct TypeParseTraits<X> {                                                                                        \
        static const FieldType type;                                                                                   \
    };                                                                                                                 \
    const FieldType TypeParseTraits<X>::type = Y

REGISTER_PARSE_TYPE(uint8_t, ft_uint8);
REGISTER_PARSE_TYPE(int8_t, ft_int8);
REGISTER_PARSE_TYPE(uint16_t, ft_uint16);
REGISTER_PARSE_TYPE(int16_t, ft_int16);
REGISTER_PARSE_TYPE(uint32_t, ft_uint32);
REGISTER_PARSE_TYPE(int32_t, ft_int32);
REGISTER_PARSE_TYPE(uint64_t, ft_uint64);
REGISTER_PARSE_TYPE(int64_t, ft_int64);
REGISTER_PARSE_TYPE(float, ft_float);
REGISTER_PARSE_TYPE(double, ft_double);
REGISTER_PARSE_TYPE(autil::MultiChar, ft_string);
REGISTER_PARSE_TYPE(autil::MultiString, ft_string);

class MultiValueAttributePatchMergerTest : public TESTBASE
{
public:
    MultiValueAttributePatchMergerTest() = default;
    ~MultiValueAttributePatchMergerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    struct Operation {
        Operation(docid_t docId_, std::string value_, bool isNull_)
        {
            docId = docId_;
            isNull = isNull_;
            value = value_;
        }
        docid_t docId = INVALID_DOCID;
        bool isNull = false;
        std::string value;
    };

private:
    std::shared_ptr<AttributeConfig> MakeAttributeConfig(FieldType type, bool multiValue);

    template <typename T>
    void InnerTest(const std::vector<std::string>& segmentPatchsStr, bool multiValue);

    template <typename T>
    void CreateOperationAndExpectResults(const std::string& operationStr, std::vector<Operation>& operations,
                                         std::map<docid_t, std::vector<T>>& expectResults);

    void ClearTestRoot();

    std::pair<std::shared_ptr<framework::Segment>, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetSegment(segmentid_t srcSegId);

    template <typename T>
    void GenPatchByUpdater(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir,
                           const std::shared_ptr<AttributeConfig>& attrConfig, const std::vector<Operation>& operations,
                           segmentid_t srcSegId);

    template <typename T>
    void Check(const std::shared_ptr<indexlib::file_system::IDirectory>& targetDir,
               const std::shared_ptr<AttributeConfig>& attrConfig, segmentid_t srcSegId,
               std::map<docid_t, std::vector<T>>& expectResults);

    template <typename T>
    void PushItem(const std::string& v, docid_t docId, std::vector<T>& values);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    const segmentid_t _dstSegId = 0;
    uint32_t _maxValueLen = 0;
    std::map<docid_t, std::vector<std::string>> _specialResults;
};

void MultiValueAttributePatchMergerTest::setUp()
{
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    if (!_rootPath.empty()) {
        indexlib::file_system::FslibWrapper::DeleteDirE(_rootPath, indexlib::file_system::DeleteOption::NoFence(true));
    }

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");

    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_ATTRIBUTE__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    _rootDir = rootDir->GetIDirectory();
    _pool.reset(new autil::mem_pool::Pool(256 * 1024));
}

void MultiValueAttributePatchMergerTest::tearDown() {}

std::shared_ptr<AttributeConfig> MultiValueAttributePatchMergerTest::MakeAttributeConfig(FieldType type,
                                                                                         bool multiValue)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig(FIELD_NAME_TAG, type, multiValue));
    fieldConfig->SetEnableNullField(true);
    std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    attrConfig->SetUpdatable(true);
    return attrConfig;
}

template <typename T>
void MultiValueAttributePatchMergerTest::PushItem(const std::string& v, docid_t docId, std::vector<T>& values)
{
    values.push_back(autil::StringUtil::fromString<T>(v));
}

template <>
void MultiValueAttributePatchMergerTest::PushItem<autil::MultiChar>(const std::string& v, docid_t docId,
                                                                    std::vector<autil::MultiChar>& values)
{
    auto& specialValues = _specialResults[docId];
    specialValues.push_back(v);
}

template <>
void MultiValueAttributePatchMergerTest::PushItem<autil::MultiString>(const std::string& v, docid_t docId,
                                                                      std::vector<autil::MultiString>& values)
{
    auto& specialValues = _specialResults[docId];
    specialValues.push_back(v);
}

template <typename T>
void MultiValueAttributePatchMergerTest::CreateOperationAndExpectResults(
    const std::string& operationStr, std::vector<Operation>& operations,
    std::map<docid_t, std::vector<T>>& expectResults)
{
    autil::StringTokenizer st;
    st.tokenize(operationStr, ";");
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        autil::StringTokenizer subSt;
        subSt.tokenize(st[i], ",");

        docid_t docId = autil::StringUtil::fromString<docid_t>(subSt[0]);
        std::vector<T> values;
        if (subSt[1] == "__NULL__") {
            Operation op(docId, "", /*isNull*/ true);
            operations.push_back(std::move(op));
            expectResults[docId] = values;
            continue;
        }

        std::string strValue;
        autil::StringTokenizer multiSt;
        multiSt.tokenize(subSt[1], "|");
        for (size_t j = 0; j < multiSt.getNumTokens(); ++j) {
            PushItem<T>(multiSt[j], docId, values);
            // if (typeid(T).name() == typeid(autil::MultiChar).name() ||
            //     typeid(T).name() == typeid(autil::MultiString).name()) {
            //     auto& specialValues = _specialResults[docId];
            //     specialValues.push_back(multiSt[j]);
            // } else {
            //     values.push_back(autil::StringUtil::fromString<T>(multiSt[j]));
            // }
            if (j != 0) {
                strValue += MULTI_VALUE_SEPARATOR;
            }
            strValue += multiSt[j];
        }
        Operation op(docId, strValue, /*isNull*/ false);
        operations.push_back(std::move(op));
        expectResults[docId] = values;
    }
}

std::pair<std::shared_ptr<framework::Segment>, std::shared_ptr<indexlib::file_system::IDirectory>>
MultiValueAttributePatchMergerTest::GetSegment(segmentid_t srcSegId)
{
    auto segName = GetSegmentName(srcSegId);
    auto [status, segDir] = _rootDir->MakeDirectory(segName, indexlib::file_system::DirectoryOption()).StatusWith();
    if (!status.IsOK()) {
        return std::make_pair(nullptr, nullptr);
    }
    framework::SegmentMeta segMeta(srcSegId);
    segMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(segDir);
    auto segment = std::make_shared<framework::FakeSegment>(framework::Segment::SegmentStatus::ST_BUILT);
    segment->_segmentMeta = segMeta;
    return std::make_pair(segment, segDir);
}

void MultiValueAttributePatchMergerTest::ClearTestRoot()
{
    auto ec = fslib::fs::FileSystem::removeDir(_rootPath);
    ec = fslib::fs::FileSystem::mkDir(_rootPath);
    (void)ec;
    tearDown();
    setUp();
}

template <typename T>
void MultiValueAttributePatchMergerTest::GenPatchByUpdater(
    const std::shared_ptr<indexlib::file_system::IDirectory>& segDir,
    const std::shared_ptr<AttributeConfig>& attrConfig, const std::vector<Operation>& operations, segmentid_t srcSegId)
{
    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    ASSERT_TRUE(convertor != nullptr);

    MultiValueAttributeUpdater<T> updater(_dstSegId, attrConfig);
    for (const auto& op : operations) {
        if (op.isNull) {
            updater.Update(op.docId, autil::StringView::empty_instance(), op.isNull);
            _maxValueLen = std::max(_maxValueLen, (uint32_t)4); // null value is 4 byte length (special count)
        } else {
            autil::StringView encodeValue = convertor->Encode(autil::StringView(op.value), _pool.get());
            AttrValueMeta meta = convertor->Decode(encodeValue);
            _maxValueLen = std::max(_maxValueLen, (uint32_t)meta.data.size());
            updater.Update(op.docId, meta.data, op.isNull);
        }
    }

    auto [st, attrDir] = segDir->MakeDirectory("attribute", indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(st.IsOK());
    ASSERT_TRUE(updater.Dump(attrDir, srcSegId++).IsOK());
}

template <typename T>
void MultiValueAttributePatchMergerTest::Check(const std::shared_ptr<indexlib::file_system::IDirectory>& targetDir,
                                               const std::shared_ptr<AttributeConfig>& attrConfig, segmentid_t srcSegId,
                                               std::map<docid_t, std::vector<T>>& expectResults)
{
    MultiValueAttributePatchFile patchFile(_dstSegId, attrConfig);
    std::stringstream ss;
    ss << srcSegId << "_" << _dstSegId << "." << PATCH_FILE_NAME;
    ASSERT_TRUE(patchFile.Open(targetDir, ss.str()).IsOK());
    ASSERT_EQ(_dstSegId, patchFile.GetSegmentId());
    for (size_t i = 0; i < expectResults.size(); ++i) {
        ASSERT_TRUE(patchFile.HasNext());
        ASSERT_TRUE(patchFile.Next().IsOK());
        docid_t docId = patchFile.GetCurDocId();
        const auto& expectedValue = expectResults[docId];
        bool isNull = false;
        // TODO
        uint8_t buffer[_maxValueLen];

        patchFile.GetPatchValue<T>(buffer, _maxValueLen, isNull);
        ASSERT_EQ(expectedValue.empty(), isNull) << docId;
        if (isNull) {
            ASSERT_TRUE(isNull);
            continue;
        }
        ASSERT_FALSE(isNull);
        size_t encodeCountLen = 0;
        uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
        T* actualValue = (T*)(buffer + encodeCountLen);
        ASSERT_EQ(expectedValue.size(), recordNum);
        for (size_t j = 0; j < expectedValue.size(); ++j) {
            ASSERT_EQ(expectedValue[j], actualValue[j]);
        }
    }
    ASSERT_FALSE(patchFile.HasNext());
    ASSERT_EQ(_maxValueLen, patchFile.GetMaxPatchItemLen());
}

template <typename T>
void MultiValueAttributePatchMergerTest::InnerTest(const std::vector<std::string>& segmentPatchsStr, bool multiValue)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    segmentid_t srcSegId = 1;
    auto attrConfig = MakeAttributeConfig(TypeParseTraits<T>::type, multiValue);
    std::map<docid_t, std::vector<T>> expectResults;
    for (const auto& patchStr : segmentPatchsStr) {
        std::vector<Operation> operations;
        CreateOperationAndExpectResults<T>(patchStr, operations, expectResults);

        auto [segment, segDir] = GetSegment(srcSegId);
        ASSERT_TRUE(segment != nullptr);
        segments.emplace_back(std::move(segment));

        GenPatchByUpdater<T>(segDir, attrConfig, operations, srcSegId++);
    }

    auto dirName = GetSegmentName(srcSegId) + "/attribute/" + FIELD_NAME_TAG;
    auto [st, targetDir] = _rootDir->MakeDirectory(dirName, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(st.IsOK());

    std::string patchFileName =
        autil::StringUtil::toString(srcSegId) + "_" + autil::StringUtil::toString(_dstSegId) + "." + PATCH_FILE_NAME;
    auto [status, destPatchFileWriter] =
        targetDir->CreateFileWriter(patchFileName, indexlib::file_system::WriterOption()).StatusWith();

    PatchInfos patchInfos;
    AttributePatchFileFinder finder;
    ASSERT_TRUE(finder.FindAllPatchFiles(segments, attrConfig, &patchInfos).IsOK());
    MultiValueAttributePatchMerger<T> merger(attrConfig, /*updateBitmap*/ nullptr);
    ASSERT_TRUE(merger.Merge(patchInfos[_dstSegId], destPatchFileWriter).IsOK());

    Check<T>(targetDir, attrConfig, srcSegId, expectResults);
    ClearTestRoot();
}

TEST_F(MultiValueAttributePatchMergerTest, testSinglePatch)
{
    std::vector<std::string> segmentPatchs {{"15,0;20,3;5,7;3,1|2|3;100,6|7|8;1025,9"}};
    InnerTest<int8_t>(segmentPatchs, /*isMulti*/ true);
}

TEST_F(MultiValueAttributePatchMergerTest, testMultiPatch)
{
    std::vector<std::string> segmentPatchs {
        {"15,0|1;20,3|1;5,7|1;3,1|1;100,6|1;1025,9"}, {"15,1|2;20,4|2;3,2|2"}, {"15,2|3;20,2|3;3,3|3;100,10|3|4"}};
    InnerTest<int8_t>(segmentPatchs, /*isMulti*/ true);
}

TEST_F(MultiValueAttributePatchMergerTest, testSinglePatch4float)
{
    std::vector<std::string> segmentPatchs {{"15,0.0;20,3.0;5,7.0;3,1.0|2.0|3.0;100,6.0|7.0|8.0;1025,9.0"}};
    InnerTest<float>(segmentPatchs, /*isMulti*/ true);
}

TEST_F(MultiValueAttributePatchMergerTest, testMultiPatch4float)
{
    std::vector<std::string> segmentPatchs {{"15,0.0|1.0;20,3.0|1.0;5,7.0|1.0;3,1.0|1.0;100,6.0|1.0;1025,9.0"},
                                            {"15,1.0|2.0;20,4.0|2.0;3,2.0|2.0"},
                                            {"15,2.0|3.0;20,2.0|3.0;3,3.0|3.0;100,10.0|3.0|4.0"}};
    InnerTest<float>(segmentPatchs, /*isMulti*/ true);
}
// TODO(xiuchen) to support
// TEST_F(MultiValueAttributePatchMergerTest, testSingleStringPatch)
// {
//     std::vector<std::string> segmentPatchs {{"10,aaa;7,bbb;9,cc;4,dddd"}, {"10,aaaa;9,ccc"}, {"9,cccc;4,ddddd"}};
//     InnerTest<autil::MultiChar>(segmentPatchs, /*isMulti*/ false);
// }

// TEST_F(MultiValueAttributePatchMergerTest, testMultiStringPatch)
// {
//     std::vector<std::string> segmentPatchs {
//         {"15,0|1;20,3|1;5,7|1;3,1|1;100,6|1;1025,9"}, {"15,1|2;20,4|2;3,2|2"}, {"15,2|3;20,2|3;3,3|3;100,10|3|4"}};
//     InnerTest<int8_t>(segmentPatchs, /*isMulti*/ true);
// }

} // namespace indexlibv2::index
