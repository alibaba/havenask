#include "indexlib/test/single_field_partition_data_provider.h"

#include "autil/StringUtil.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace test {
IE_LOG_SETUP(test, SingleFieldPartitionDataProvider);

#define DEFAULT_PK_FIELD_NAME "pk_field"
#define PK_INDEX_NAME "pk_index"
#define FIELD_NAME_TAG "field"

const std::string SingleFieldPartitionDataProvider::INDEX_NAME_TAG = "index";

SingleFieldPartitionDataProvider::SingleFieldPartitionDataProvider() : mType(SFP_UNKNOW), mDocCount(0), mAddDocCount(0)
{
    mOptions.SetEnablePackageFile(false);
}

SingleFieldPartitionDataProvider::SingleFieldPartitionDataProvider(const config::IndexPartitionOptions& options)
    : mOptions(options)
    , mType(SFP_UNKNOW)
    , mDocCount(0)
    , mAddDocCount(0)
{
}

void SingleFieldPartitionDataProvider::Init(const string& rootDir, const string& fieldTypeStr, SFP_IndexType type,
                                            bool enableNull)
{
    mRootDir = rootDir;
    InitSchema(fieldTypeStr, type);
    if (enableNull) {
        SchemaMaker::EnableNullFields(mSchema, FIELD_NAME_TAG);
    }
    mType = type;
    assert(mType != SFP_UNKNOW);
}

// docStrings:    [segDocStr#segDocStr#...]
// segDocStr:     [docFieldValue,docFieldValue,...]
// docFieldValue: [addDoc|delDoc|updateDoc]
//    addDoc:     add:fieldValue or fieldValue
//    delDoc:     delete:pkValue
//    updateDoc:  update_field:pkValue:fieldValue (pkValue is docId)
bool SingleFieldPartitionDataProvider::Build(const string& docStrings, SFP_BuildType buildType)
{
    IndexBuilderPtr builder = CreateIndexBuilder(buildType);
    if (!builder) {
        return false;
    }
    vector<string> segmentDocStrings;
    StringUtil::fromString(docStrings, segmentDocStrings, "#");
    for (size_t i = 0; i < segmentDocStrings.size(); i++) {
        if (buildType == SFP_REALTIME && builder->GetIndexPartition()->GetWriter()) {
            // dump last building segment
            builder->DumpSegment();
        }

        BuildOneSegmentData(builder, segmentDocStrings[i]);
        if (buildType == SFP_OFFLINE) {
            // dump current segment for offline build
            builder->DumpSegment();
        }
    }
    int64_t versionTs = 0;
    builder->EndIndex(versionTs);
    builder->TEST_BranchFSMoveToMainRoad();
    return true;
}

IndexConfigPtr SingleFieldPartitionDataProvider::GetIndexConfig()
{
    assert(mType == SFP_PK_INDEX || mType == SFP_PK_128_INDEX || mType == SFP_INDEX || mType == SFP_TRIE);
    string indexName = (mType == SFP_INDEX) ? INDEX_NAME_TAG : PK_INDEX_NAME;
    return mSchema->GetIndexSchema()->GetIndexConfig(indexName);
}

AttributeConfigPtr SingleFieldPartitionDataProvider::GetAttributeConfig()
{
    assert(mType == SFP_ATTRIBUTE);
    return mSchema->GetAttributeSchema()->GetAttributeConfig(FIELD_NAME_TAG);
}

void SingleFieldPartitionDataProvider::BuildOneSegmentData(const IndexBuilderPtr& builder, const string& docString)
{
    vector<NormalDocumentPtr> docs = CreateDocuments(docString);
    for (size_t i = 0; i < docs.size(); ++i) {
        builder->Build(docs[i]);
    }
}

vector<NormalDocumentPtr> SingleFieldPartitionDataProvider::CreateDocuments(const string& docString)
{
    vector<string> docStrs;
    string normalizedDocString = "";
    string docSep = ",";
    if (mSpatialField) {
        docSep = "|";
    }
    StringUtil::fromString(docString, docStrs, docSep);
    for (size_t i = 0; i < docStrs.size(); i++) {
        normalizedDocString += NormalizeOneDocString(docStrs[i]);
    }

    vector<NormalDocumentPtr> docs = DocumentCreator::CreateNormalDocuments(mSchema, normalizedDocString);
    for (size_t i = 0; i < docs.size(); i++) {
        document::Locator locator(StringUtil::toString(mDocCount));
        docs[i]->SetLocator(locator);
        docs[i]->SetTimestamp(mDocCount);
        mDocCount++;
    }
    return docs;
}

IndexBuilderPtr SingleFieldPartitionDataProvider::CreateIndexBuilder(SFP_BuildType buildType)
{
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/1, &mOptions);
    if (buildType == SFP_OFFLINE) {
        assert(!mOnlinePartition);
        IndexBuilderPtr indexBuilder(
            new IndexBuilder(mRootDir, mOptions, mSchema, quotaControl, BuilderBranchHinter::Option::Test()));
        if (!indexBuilder->Init()) {
            return IndexBuilderPtr();
        }
        return indexBuilder;
    }

    assert(buildType == SFP_REALTIME);
    if (!mOnlinePartition) {
        mOptions.SetIsOnline(true);
        mOnlinePartition.reset(new partition::OnlinePartition);
        IndexPartition::OpenStatus os = mOnlinePartition->Open(mRootDir, "", mSchema, mOptions);
        if (os != IndexPartition::OS_OK) {
            return IndexBuilderPtr();
        }
        // online using rewrited online schema
        mSchema = mOnlinePartition->GetSchema();
    }

    IndexBuilderPtr indexBuilder(new IndexBuilder(mOnlinePartition, quotaControl));
    if (!indexBuilder->Init()) {
        return IndexBuilderPtr();
    }
    return indexBuilder;
}

PartitionDataPtr SingleFieldPartitionDataProvider::GetPartitionData()
{
    if (mOnlinePartition) {
        return mOnlinePartition->GetPartitionData();
    }

    mOptions.SetIsOnline(false);
    DumpSegmentContainerPtr dumpSegContainer(new DumpSegmentContainer);
    FileSystemOptions fsOptions = FileSystemFactory::CreateFileSystemOptions(
        mRootDir, mOptions, MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
        FileBlockCacheContainerPtr(), "");
    auto fileSystem = FileSystemCreator::Create("SingleFieldPartitionDataProvider", mRootDir, fsOptions).GetOrThrow();
    int32_t id = -1;
    GetEntryTableId(mRootDir, id);
    if (id != -1) {
        THROW_IF_FS_ERROR(fileSystem->MountVersion(mRootDir, id, "", FSMT_READ_ONLY, nullptr), "mount version failed");
    } else {
        THROW_IF_FS_ERROR(fileSystem->MountDir(mRootDir, "", "", FSMT_READ_WRITE, true), "mount dir failed");
    }
    util::MetricProviderPtr metricProvider;
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    auto memoryController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    BuildingPartitionParam param(mOptions, mSchema, memoryController, dumpSegContainer, counterMap, pluginManager,
                                 metricProvider, document::SrcSignature());
    return PartitionDataCreator::CreateBuildingPartitionData(param, fileSystem, index_base::Version(INVALID_VERSION),
                                                             "", index_base::InMemorySegmentPtr());
}

void SingleFieldPartitionDataProvider::InitSchema(const string& fieldTypeStr, SFP_IndexType type)
{
    string fields = string(FIELD_NAME_TAG) + ":" + fieldTypeStr;
    string index = "";

    if (type == SFP_PK_INDEX) {
        index = string(PK_INDEX_NAME) + ":primarykey64:" + FIELD_NAME_TAG;
    } else if (type == SFP_PK_128_INDEX) {
        index = string(PK_INDEX_NAME) + ":primarykey128:" + FIELD_NAME_TAG;
    } else if (type == SFP_TRIE) {
        index = string(PK_INDEX_NAME) + ":trie:" + FIELD_NAME_TAG;
    } else {
        // add default pk
        fields = string(DEFAULT_PK_FIELD_NAME) + string(":uint32;") + fields;
        index = string(PK_INDEX_NAME) + ":primarykey64:" + DEFAULT_PK_FIELD_NAME;
    }

    if (type == SFP_INDEX) {
        if (fieldTypeStr == "location") {
            index = INDEX_NAME_TAG + ":spatial:" + FIELD_NAME_TAG + ";" + index;
        } else if (fieldTypeStr == "int64") {
            index = INDEX_NAME_TAG + ":" + "number" + ":" + FIELD_NAME_TAG + ";" + index;
        } else {
            index = INDEX_NAME_TAG + ":" + fieldTypeStr + ":" + FIELD_NAME_TAG + ";" + index;
        }
    }

    string attribute = "";
    if (type == SFP_ATTRIBUTE) {
        attribute = FIELD_NAME_TAG;
    }

    // TODO: support summary
    assert(type != SFP_SUMMARY);
    mSchema = SchemaMaker::MakeSchema(fields, index, attribute, "");
}

//    addDoc:     add:fieldValue or fieldValue
//    delDoc:     delete:pkValue
//    updateDoc:  update_field:pkValue:fieldValue
string SingleFieldPartitionDataProvider::NormalizeOneDocString(const string& oneDocStr)
{
    vector<string> fieldItems;
    StringUtil::fromString(oneDocStr, fieldItems, ":");
    string cmd_type = (fieldItems.size() == 1) ? CMD_ADD : fieldItems[0];
    if (cmd_type == CMD_ADD) {
        string fieldValue = (fieldItems.size() == 1) ? fieldItems[0] : fieldItems[1];
        return CreateAddDocumentStr(fieldValue);
    } else if (cmd_type == CMD_DELETE) {
        assert(fieldItems.size() == 2);
        return CreateDeleteDocumentStr(fieldItems[1]);
    } else if (cmd_type == CMD_UPDATE_FIELD) {
        assert(fieldItems.size() == 3);
        return CreateUpdateDocumentStr(fieldItems[1], fieldItems[2]);
    }
    assert(false);
    return "";
}

string SingleFieldPartitionDataProvider::CreateUpdateDocumentStr(const string& pkValue, const string& fieldValue)
{
    string fieldSep = ",";
    if (mSpatialField) {
        fieldSep = "|";
    }
    stringstream ss;
    ss << CMD_TAG << "=" << CMD_UPDATE_FIELD << fieldSep << FIELD_NAME_TAG << "=" << fieldValue << fieldSep
       << DEFAULT_PK_FIELD_NAME << "=" << pkValue << ";";
    return ss.str();
}

string SingleFieldPartitionDataProvider::CreateAddDocumentStr(const string& fieldValue)
{
    string fieldSep = ",";
    if (mSpatialField) {
        fieldSep = "|";
    }
    stringstream ss;
    ss << CMD_TAG << "=" << CMD_ADD << fieldSep << FIELD_NAME_TAG << "=" << fieldValue;

    if (!IsPrimaryKey()) {
        ss << fieldSep << DEFAULT_PK_FIELD_NAME << "=" << mAddDocCount++;
    }
    ss << ";";
    return ss.str();
}

string SingleFieldPartitionDataProvider::CreateDeleteDocumentStr(const string& fieldValue)
{
    string fieldSep = ",";
    if (mSpatialField) {
        fieldSep = "|";
    }
    stringstream ss;
    ss << CMD_TAG << "=" << CMD_DELETE << fieldSep;
    if (!IsPrimaryKey()) {
        ss << DEFAULT_PK_FIELD_NAME;
    } else {
        ss << FIELD_NAME_TAG;
    }
    ss << "=" << fieldValue << ";";
    return ss.str();
}

void SingleFieldPartitionDataProvider::Merge(const string& mergedSegmentIdsStr)
{
    MergeConfig& mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString(string("merge_segments=") + mergedSegmentIdsStr);

    mOptions.SetIsOnline(false);
    BuildConfig& buildConfig = mOptions.GetBuildConfig();
    buildConfig.keepVersionCount = 100;

    assert(!mOnlinePartition);
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilderPtr indexBuilder(
        new IndexBuilder(mRootDir, mOptions, mSchema, quotaControl, BuilderBranchHinter::Option::Test()));
    if (!indexBuilder->Init()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index builder init fail");
    }
    indexBuilder->Merge(mOptions, false);
    mOptions.SetIsOnline(true);
}

bool SingleFieldPartitionDataProvider::IsPrimaryKey() const
{
    return mType == SFP_PK_INDEX || mType == SFP_PK_128_INDEX || mType == SFP_TRIE;
}

void SingleFieldPartitionDataProvider::GetEntryTableId(const std::string& path, int32_t& id) const
{
    fslib::FileList fileList;
    auto ec = FslibWrapper::ListDir(PathUtil::NormalizeDir(path), fileList).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "list entryTable in [%s] failed, ec [%d]", path.c_str(), ec);
    }

    std::string key = ENTRY_TABLE_FILE_NAME_PREFIX;
    for (const auto& file : fileList) {
        int32_t tmpId = 0;
        if (file.find(key) != 0) {
            continue;
        }
        size_t prefixLen = key.length() + 1;
        if (file.length() < prefixLen || file[prefixLen - 1] != '.') {
            continue;
        }

        string strNum = file.substr(prefixLen);
        if (!StringUtil::strToInt32(strNum.c_str(), tmpId)) {
            continue;
        }
        id = std::max(tmpId, id);
    }
    return;
}
}} // namespace indexlib::test
