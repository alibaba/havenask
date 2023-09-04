#include "indexlib/index/attribute/test/AttributeTestUtil.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/util/Random.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace fslib;
using namespace fslib::fs;
using indexlibv2::config::FieldConfig;

namespace indexlibv2::index {

///////////////////////////////////////////////////////
// methods of AttributeTestUtil

void AttributeTestUtil::CreateDocCounts(uint32_t segmentCount, vector<uint32_t>& docCounts)
{
    docCounts.clear();
    for (uint32_t i = 0; i < segmentCount; ++i) {
        uint32_t docCount = indexlib::util::dev_urandom() % 20 + 10;
        docCounts.push_back(docCount);
    }
}

void AttributeTestUtil::CreateEmptyDelSets(const vector<uint32_t>& docCounts, vector<set<docid_t>>& delDocIdSets)
{
    delDocIdSets.clear();
    for (size_t i = 0; i < docCounts.size(); ++i) {
        delDocIdSets.push_back(set<docid_t>());
    }
}

void AttributeTestUtil::CreateDelSets(const vector<uint32_t>& docCounts, vector<set<docid_t>>& delDocIdSets)
{
    delDocIdSets.clear();
    for (size_t i = 0; i < docCounts.size(); ++i) {
        uint32_t docCount = docCounts[i];
        set<docid_t> delDocIdSet;
        for (uint32_t j = 0; j < docCount; ++j) {
            if (indexlib::util::dev_urandom() % 2 == 1) {
                delDocIdSet.insert(j);
            }
        }
        delDocIdSets.push_back(delDocIdSet);
    }
}

std::shared_ptr<AttributeConfig> AttributeTestUtil::CreateAttrConfig(FieldType fieldType, bool isMultiValue,
                                                                     const string& fieldName, fieldid_t fid,
                                                                     std::optional<std::string> compressType,
                                                                     bool supportNull, bool isUpdatable,
                                                                     uint64_t offsetThreshold)
{
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig(fieldName, fieldType, isMultiValue));
    fieldConfig->SetFieldId(fid);

    fieldConfig->SetEnableNullField(supportNull);
    std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }

    if (offsetThreshold != index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX) {
        attrConfig->SetU32OffsetThreshold(offsetThreshold);
    }
    attrConfig->SetAttrId(0);
    if (isMultiValue || fieldType == ft_string) {
        attrConfig->SetUpdatable(isUpdatable);
    }
    if (compressType != nullopt) {
        auto status = attrConfig->SetCompressType(compressType.value());
        if (!status.IsOK()) {
            return nullptr;
        }
    }
    return attrConfig;
}

std::shared_ptr<AttributeConfig> AttributeTestUtil::CreateAttrConfig(FieldType fieldType, bool isMultiValue,
                                                                     const string& fieldName, fieldid_t fid,
                                                                     const string& compressTypeStr, int32_t fixedLen)
{
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig(fieldName, fieldType, isMultiValue));
    fieldConfig->SetFieldId(fid);
    assert(fixedLen != 0);
    fieldConfig->SetFixedMultiValueCount(fixedLen);

    std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    attrConfig->SetAttrId(0);
    status = attrConfig->SetCompressType(compressTypeStr);
    if (!status.IsOK()) {
        return nullptr;
    }
    return attrConfig;
}

std::shared_ptr<AttributeConvertor>
AttributeTestUtil::CreateAttributeConvertor(std::shared_ptr<AttributeConfig> attrConfig)
{
    return std::shared_ptr<AttributeConvertor>(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
}

void AttributeTestUtil::DumpWriter(AttributeMemIndexer& writer, const string& dirPath,
                                   const std::shared_ptr<framework::DumpParams>& dumpParams,
                                   std::shared_ptr<indexlib::file_system::Directory>& outputDir)
{
    FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    std::shared_ptr<MemoryQuotaController> mqc(new MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    std::shared_ptr<PartitionMemoryQuotaController> quotaController(new PartitionMemoryQuotaController(mqc));
    fileSystemOptions.memoryQuotaController = quotaController;
    std::shared_ptr<IFileSystem> fileSystem =
        FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                  /*rootPath=*/dirPath, fileSystemOptions,
                                  std::shared_ptr<indexlib::util::MetricProvider>(),
                                  /*isOverride=*/false)
            .GetOrThrow();
    outputDir = IDirectory::ToLegacyDirectory(make_shared<MemDirectory>("", fileSystem));
    SimplePool dumpPool;
    [[maybe_unused]] auto status = writer.Dump(&dumpPool, outputDir, dumpParams);
    assert(status.IsOK());
    fileSystem->Sync(true).GetOrThrow();
}

void AttributeTestUtil::ResetDir(const string& dir)
{
    CleanDir(dir);
    MkDir(dir);
}

void AttributeTestUtil::MkDir(const string& dir)
{
    if (!FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            FslibWrapper::MkDirE(dir, true);
        } catch (const FileIOException&) {
            std::cout << "Create work directory: [" << dir << "] FAILED" << std::endl;
        }
    }
}

void AttributeTestUtil::CleanDir(const string& dir)
{
    if (FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            FslibWrapper::DeleteDirE(dir, DeleteOption::NoFence(false));
        } catch (const FileIOException&) {
            std::cout << "Remove directory: [" << dir << "] FAILED." << std::endl;
            assert(false);
        }
    }
}

std::shared_ptr<IFileSystem> AttributeTestUtil::CreateFileSystem(const std::string& rootPath)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    return indexlib::file_system::FileSystemCreator::Create("ut_fs", rootPath, fsOptions).GetOrThrow();
}

} // namespace indexlibv2::index
