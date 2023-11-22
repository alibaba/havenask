#include "indexlib/index/test/IndexTestHelper.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/mock/FakeTabletSchema.h"
#include "indexlib/config/test/FieldConfigMaker.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/mock/FakeDiskSegment.h"
#include "indexlib/framework/mock/FakeMemSegment.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/MemIndexerParameter.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, IndexTestHelper);

struct IndexTestHelper::Impl {
    std::string tableName = "test_helper";
    std::string localRoot;
    std::string remoteRoot;
    std::shared_ptr<file_system::IDirectory> rootDirectory;
    std::shared_ptr<indexlibv2::config::FakeTabletSchema> schema;
    std::shared_ptr<indexlibv2::framework::FakeMemSegment> buildingMemSegment;
    std::shared_ptr<indexlibv2::index::MemIndexerParameter> memIndexerParameter;
    std::shared_ptr<indexlibv2::index::DiskIndexerParameter> indexerParameter;
    std::shared_ptr<indexlibv2::index::IndexReaderParameter> indexReaderParameter;
    std::map<segmentid_t, std::shared_ptr<indexlibv2::framework::FakeMemSegment>> memSegments;   // holder
    std::map<segmentid_t, std::shared_ptr<indexlibv2::framework::FakeDiskSegment>> diskSegments; // holder
    std::vector<segmentid_t> builtDiskSegmentIds;
    std::vector<segmentid_t> dumpingMemSegmentIds;
    segmentid_t nextSegmentId = 0;
};

IndexTestHelper::IndexTestHelper() : _impl(std::make_unique<IndexTestHelper::Impl>())
{
    _impl->schema = std::make_shared<indexlibv2::config::FakeTabletSchema>();
    _impl->memIndexerParameter = std::make_shared<indexlibv2::index::MemIndexerParameter>();
    _impl->indexerParameter = std::make_shared<indexlibv2::index::DiskIndexerParameter>();
    _impl->indexReaderParameter = std::make_shared<indexlibv2::index::IndexReaderParameter>();
}

IndexTestHelper::~IndexTestHelper() { Close(); }

Status IndexTestHelper::MakeFieldConfigs(const std::string& fieldsDesc)
{
    _impl->schema->MutableFieldConfigs() = indexlibv2::config::FieldConfigMaker::Make(fieldsDesc);
    return Status::OK();
}

Status IndexTestHelper::MakeIndexConfig(const std::string& indexDesc)
{
    auto status = DoMakeIndexConfig(indexDesc, 0);
    if (!status.IsOK()) {
        return status.steal_error();
    }
    _indexConfig = status.steal_value();
    assert(_impl->schema->MutableIndexConfigs().empty()); // just support one index config
    _impl->schema->MutableIndexConfigs().push_back(_indexConfig);
    return Status::OK();
}

Status IndexTestHelper::MakeSchema(const std::string& fieldsDesc, const std::string& indexDesc)
{
    RETURN_IF_STATUS_ERROR(MakeFieldConfigs(fieldsDesc), "make field configs failed");
    RETURN_IF_STATUS_ERROR(MakeIndexConfig(indexDesc), "make index config failed");
    return Status::OK();
}

segmentid_t IndexTestHelper::NextSegmentId() { return _impl->nextSegmentId++; }
StatusOr<std::shared_ptr<indexlibv2::framework::FakeMemSegment>> IndexTestHelper::NewMemSegment(segmentid_t segmentId)
{
    auto memIndexerRt = CreateMemIndexer();
    if (!memIndexerRt.IsOK()) {
        return memIndexerRt.steal_error();
    }
    auto memIndexer = memIndexerRt.steal_value();
    indexlibv2::framework::SegmentMeta segmentMeta;
    segmentMeta.segmentId = segmentId;
    segmentMeta.schema = _impl->schema;
    std::string segmentDirName = indexlibv2::PathUtil::NewSegmentDirName(segmentMeta.segmentId, 0);
    auto segmentDirectory = _impl->rootDirectory->MakeDirectory(segmentDirName, {});
    if (!segmentDirectory.OK()) {
        return segmentDirectory.Status().steal_error();
    }
    segmentMeta.segmentDir = file_system::IDirectory::ToLegacyDirectory(segmentDirectory.Value());
    auto memSegment = std::make_shared<indexlibv2::framework::FakeMemSegment>(segmentMeta);
    memSegment->AddIndexer(_indexConfig->GetIndexType(), _indexConfig->GetIndexName(), memIndexer);
    _impl->memSegments.try_emplace(segmentId, memSegment);
    return memSegment;
}

StatusOr<std::shared_ptr<indexlibv2::framework::FakeDiskSegment>>
IndexTestHelper::NewDiskSegment(segmentid_t segmentId,
                                const std::shared_ptr<indexlibv2::framework::SegmentInfo>& segmentinfo)
{
    indexlibv2::framework::SegmentMeta segmentMeta;
    segmentMeta.segmentId = segmentId;
    segmentMeta.schema = _impl->schema;
    segmentMeta.segmentInfo = segmentinfo;
    std::string segmentDirName = indexlibv2::PathUtil::NewSegmentDirName(segmentMeta.segmentId, 0);
    auto [status, segmentDirectory] = _impl->rootDirectory->GetDirectory(segmentDirName).StatusWith();
    if (!status.IsOK()) {
        return status.steal_error();
    }
    segmentMeta.segmentDir = file_system::IDirectory::ToLegacyDirectory(segmentDirectory);
    auto diskSegment = std::make_shared<indexlibv2::framework::FakeDiskSegment>(segmentMeta);
    diskSegment->AddIndexer(_indexConfig->GetIndexType(), _indexConfig->GetIndexName(), nullptr);
    if (auto status = diskSegment->Open(nullptr, indexlibv2::framework::DiskSegment::OpenMode::NORMAL);
        !status.IsOK()) {
        return status.steal_error();
    }
    _impl->diskSegments.try_emplace(segmentId, diskSegment);
    return diskSegment;
}

Status IndexTestHelper::MakeTabletData()
{
    auto tabletData = std::make_shared<indexlibv2::framework::TabletData>(GetTableName());
    auto schemaGroup = std::make_shared<indexlibv2::framework::TabletDataSchemaGroup>();
    schemaGroup->writeSchema = _impl->schema;
    schemaGroup->onDiskWriteSchema = _impl->schema;
    schemaGroup->onDiskReadSchema = _impl->schema;
    auto resourceMap = std::make_shared<indexlibv2::framework::ResourceMap>();
    if (auto status = resourceMap->AddVersionResource(indexlibv2::framework::TabletDataSchemaGroup::NAME, schemaGroup);
        !status.IsOK()) {
        AUTIL_LOG(ERROR, "add schema to resource map failed, status[%s]", status.ToString().c_str());
        return status;
    }
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (const auto& segmentId : _impl->builtDiskSegmentIds) {
        if (auto it = _impl->diskSegments.find(segmentId); it != _impl->diskSegments.end()) {
            segments.push_back(it->second);
        } else {
            return Status::InvalidArgs("segmentId[%d] not in diskSegments", segmentId);
        }
    }
    for (const auto& segmentId : _impl->dumpingMemSegmentIds) {
        if (auto it = _impl->memSegments.find(segmentId); it != _impl->memSegments.end()) {
            segments.push_back(it->second);
        } else {
            return Status::InvalidArgs("segmentId[%d] not in memSegments", segmentId);
        }
    }
    if (_impl->buildingMemSegment) {
        segments.push_back(_impl->buildingMemSegment);
    }
    if (auto status = tabletData->Init(indexlibv2::framework::Version(), segments, resourceMap); !status.IsOK()) {
        AUTIL_LOG(ERROR, "init tablet data failed, status[%s]", status.ToString().c_str());
        return status;
    }
    _tabletData = tabletData;
    return Status::OK();
}

Status IndexTestHelper::Open(const std::string& localRoot, const std::string& remoteRoot)
{
    // file system & directory
    _impl->localRoot = localRoot;
    _impl->remoteRoot = remoteRoot;
    file_system::FileSystemOptions fsOptions;
    auto fs = file_system::FileSystemCreator::Create("test_online", localRoot, fsOptions);
    if (!fs.OK()) {
        return fs.Status();
    }
    _impl->rootDirectory = file_system::IDirectory::Get(fs.Value());
    // tablet data
    if (auto status = MakeTabletData(); !status.IsOK()) {
        return status;
    }
    // init index test helper
    DoInit();
    // return DoOpen();
    return Status::OK();
}

// Building MemSegment
StatusOr<segmentid_t> IndexTestHelper::Build(const std::string& docs)
{
    if (!_impl->buildingMemSegment) {
        auto segmentId = NextSegmentId();
        if (auto status = NewMemSegment(segmentId); !status.IsOK()) {
            return status.steal_error();
        } else {
            PushBuildingSegment(status.steal_value());
        }
        if (auto status = DoOpen(); !status.IsOK()) {
            return status.steal_error();
        }
    }
    const auto& documentBatch = MakeDocumentBatch(docs);
    if (auto status = _impl->buildingMemSegment->Build(documentBatch.get()); !status.IsOK()) {
        return status.steal_error();
    }
    return _impl->buildingMemSegment->GetSegmentId();
}

// Building --> Dumping
StatusOr<segmentid_t> IndexTestHelper::Dump()
{
    _impl->buildingMemSegment->SetSegmentStatus(indexlibv2::framework::Segment::SegmentStatus::ST_DUMPING);
    auto segmentId = _impl->buildingMemSegment->GetSegmentId();
    if (auto status = _impl->buildingMemSegment->Dump(); !status.IsOK()) {
        return status.steal_error();
    }
    _impl->buildingMemSegment.reset();
    if (auto status = DoOpen(); !status.IsOK()) {
        return status.steal_error();
    }
    return segmentId;
}

// Dumping MemSegment
StatusOr<segmentid_t> IndexTestHelper::BuildDumpingSegment(const std::string& docs)
{
    const auto& documentBatch = MakeDocumentBatch(docs);
    auto segmentId = NextSegmentId();
    auto memSegmentRt = NewMemSegment(segmentId);
    if (!memSegmentRt.IsOK()) {
        return memSegmentRt.steal_error();
    }
    auto memSegment = memSegmentRt.steal_value();
    if (auto status = memSegment->Build(documentBatch.get()); !status.IsOK()) {
        return status.steal_error();
    }
    if (auto status = memSegment->Dump(); !status.IsOK()) {
        return status.steal_error();
    }
    PushDumpingSegment(memSegment);
    if (auto status = ReOpen(); !status.IsOK()) {
        return status.steal_error();
    }
    return segmentId;
}

// Built DiskSegment
StatusOr<segmentid_t> IndexTestHelper::BuildDiskSegment(const std::string& docs)
{
    const auto& documentBatch = MakeDocumentBatch(docs);
    auto segmentId = NextSegmentId();
    auto memSegmentRt = NewMemSegment(segmentId);
    if (!memSegmentRt.IsOK()) {
        return memSegmentRt.steal_error();
    }
    auto memSegment = memSegmentRt.steal_value();
    if (auto status = memSegment->Build(documentBatch.get()); !status.IsOK()) {
        AUTIL_LOG(ERROR, "build mem segment failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    if (auto status = memSegment->Dump(); !status.IsOK()) {
        AUTIL_LOG(ERROR, "dump mem segment failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    if (auto status = NewDiskSegment(segmentId, memSegment->GetSegmentInfo()); !status.IsOK()) {
        AUTIL_LOG(ERROR, "new disk segment failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    } else {
        PushBuiltSegment(status.steal_value());
    }
    if (auto status = ReOpen(); !status.IsOK()) {
        AUTIL_LOG(ERROR, "ReOpen failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    return segmentId;
}

void IndexTestHelper::PushBuildingSegment(
    const std::shared_ptr<indexlibv2::framework::FakeMemSegment>& buildingMemSegment)
{
    _impl->buildingMemSegment = buildingMemSegment;
    _tabletData->TEST_PushSegment(buildingMemSegment);
}
void IndexTestHelper::PushDumpingSegment(
    const std::shared_ptr<indexlibv2::framework::FakeMemSegment>& dumpingMemSegment)
{
    _impl->dumpingMemSegmentIds.push_back(dumpingMemSegment->GetSegmentId());
}
void IndexTestHelper::PushBuiltSegment(const std::shared_ptr<indexlibv2::framework::FakeDiskSegment>& builtDiskSegment)
{
    _impl->builtDiskSegmentIds.push_back(builtDiskSegment->GetSegmentId());
}

StatusOr<indexlibv2::index::IIndexMerger::SegmentMergeInfos>
IndexTestHelper::CreateSegmentMergeInfos(const std::vector<segmentid_t>& sourceSegmentIds,
                                         const std::vector<segmentid_t>& targetSegmentIds)
{
    indexlibv2::index::IIndexMerger::SegmentMergeInfos segmentMergeInfos;
    docid_t baseDocid = 0;
    std::set<segmentid_t> sourceSegmentIdSet(sourceSegmentIds.begin(), sourceSegmentIds.end());
    for (const auto& segmentId : _impl->builtDiskSegmentIds) {
        if (sourceSegmentIdSet.count(segmentId) > 0) {
            assert(_impl->diskSegments.count(segmentId) > 0);
            const auto& segment = _impl->diskSegments[segmentId];
            segmentMergeInfos.srcSegments.emplace_back(
                indexlibv2::index::IIndexMerger::SourceSegment {baseDocid, segment});
        }
        baseDocid += _impl->diskSegments[segmentId]->GetSegmentInfo()->GetDocCount();
    }
    auto minTargetSegmentId = NextSegmentId();
    for (const auto& targetSegmentId : targetSegmentIds) {
        if (targetSegmentId < minTargetSegmentId) {
            AUTIL_LOG(ERROR, "target segment id [%d] too small, min [%d]", targetSegmentId, _impl->nextSegmentId);
            return Status::InvalidArgs("target segment id [%d] too small, min [%d]", targetSegmentId,
                                       _impl->nextSegmentId);
        }
        _impl->nextSegmentId = std::max(_impl->nextSegmentId, targetSegmentId + 1);

        auto targetSegmentMeta = std::make_shared<indexlibv2::framework::SegmentMeta>(targetSegmentId);
        std::string targetSegmentDirName = indexlibv2::PathUtil::NewSegmentDirName(targetSegmentId, 0);
        auto [status, targetSegmentDirectory] =
            _impl->rootDirectory->MakeDirectory(targetSegmentDirName, {}).StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "crate targetSegmentDirectory failed, status[%s]", status.ToString().c_str());
            return status.steal_error();
        }
        targetSegmentMeta->segmentDir = file_system::IDirectory::ToLegacyDirectory(targetSegmentDirectory);
        targetSegmentMeta->segmentInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
        segmentMergeInfos.targetSegments.emplace_back(targetSegmentMeta);
    }
    // segmentMergeInfos.relocatableGlobalRoot
    return segmentMergeInfos;
}

StatusOr<std::vector<segmentid_t>> IndexTestHelper::Merge(std::vector<segmentid_t> sourceSegmentIds,
                                                          std::vector<segmentid_t> targetSegmentIds,
                                                          const std::string& docMapStr)
{
    auto indexMergerRt = CreateIndexMerger();
    if (!indexMergerRt.IsOK()) {
        AUTIL_LOG(ERROR, "CreateIndexMerger failed, status[%s]", indexMergerRt.ToString().c_str());
        return indexMergerRt.steal_error();
    }
    auto indexMerger = indexMergerRt.steal_value();
    // check sourceSegmentIds
    std::set<segmentid_t> currentSegmentIdSet(_impl->builtDiskSegmentIds.begin(), _impl->builtDiskSegmentIds.end());
    for (const auto& segmentId : sourceSegmentIds) {
        if (currentSegmentIdSet.count(segmentId) == 0) {
            AUTIL_LOG(ERROR, "segmentId[%d] is not exist in builtDiskSegmentIds", segmentId);
            return Status::InvalidArgs();
        }
    }
    // fill SegmentMergeInfos
    if (targetSegmentIds.empty()) {
        targetSegmentIds.push_back(_impl->nextSegmentId);
    }
    auto segmentMergeInfosRt = CreateSegmentMergeInfos(sourceSegmentIds, targetSegmentIds);
    if (!segmentMergeInfosRt.IsOK()) {
        return segmentMergeInfosRt.steal_error();
    }
    auto& segmentMergeInfos = segmentMergeInfosRt.get();
    // prepare IndexTaskResourceManager
    auto indexTaskResourceManagerRt = CreateIndexTaskResourceManager(segmentMergeInfos, docMapStr);
    if (!indexTaskResourceManagerRt.IsOK()) {
        AUTIL_LOG(ERROR, "file merge resource failed, status[%s]", indexTaskResourceManagerRt.ToString().c_str());
        return indexTaskResourceManagerRt.steal_error();
    }
    auto indexTaskResourceManager = indexTaskResourceManagerRt.steal_value();
    if (auto status = indexMerger->Merge(segmentMergeInfos, indexTaskResourceManager); !status.IsOK()) {
        AUTIL_LOG(ERROR, "merge failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    // open merged segment
    for (const auto& targetSegmentMeta : segmentMergeInfos.targetSegments) {
        if (auto status = NewDiskSegment(targetSegmentMeta->segmentId, targetSegmentMeta->segmentInfo);
            !status.IsOK()) {
            AUTIL_LOG(ERROR, "new disk segment failed, status[%s]", status.ToString().c_str());
            return status.steal_error();
        }
    }
    // pop sourceSegmentIds, push targetSegmentIds, ReOpen()
    std::vector<segmentid_t> newBuiltDiskSegmentIds;
    std::set<segmentid_t> sourceSegmentIdSet(sourceSegmentIds.begin(), sourceSegmentIds.end());
    for (const auto& segmentId : _impl->builtDiskSegmentIds) {
        if (sourceSegmentIdSet.count(segmentId) == 0) {
            newBuiltDiskSegmentIds.push_back(segmentId);
        }
    }
    newBuiltDiskSegmentIds.insert(newBuiltDiskSegmentIds.end(), targetSegmentIds.begin(), targetSegmentIds.end());
    _impl->builtDiskSegmentIds.swap(newBuiltDiskSegmentIds);
    if (auto status = ReOpen(); !status.IsOK()) {
        AUTIL_LOG(ERROR, "ReOpen failed, status[%s]", status.ToString().c_str());
        return status.steal_error();
    }
    return targetSegmentIds;
}

Status IndexTestHelper::ReOpen()
{
    if (auto status = MakeTabletData(); !status.IsOK()) {
        return status;
    }
    return DoOpen();
}
Status IndexTestHelper::ReOpen(std::vector<segmentid_t> builtDiskSegmentIds,
                               std::vector<segmentid_t> dumpingMemSegmentIds)
{
    _impl->builtDiskSegmentIds = builtDiskSegmentIds;
    _impl->dumpingMemSegmentIds = dumpingMemSegmentIds;
    return ReOpen();
}

bool IndexTestHelper::Query(std::string queryStr, std::string expectValue) { return DoQuery(queryStr, expectValue); }

void IndexTestHelper::Close() {}

const std::string& IndexTestHelper::GetTableName() const { return _impl->tableName; }
const std::shared_ptr<file_system::IDirectory>& IndexTestHelper::GetRootDirectory() const
{
    return _impl->rootDirectory;
}
segmentid_t IndexTestHelper::GetBuildingSegmentId() const
{
    return _impl->buildingMemSegment ? _impl->buildingMemSegment->GetSegmentId() : INVALID_SEGMENTID;
}
std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> IndexTestHelper::GetIndexConfigs() const
{
    return _impl->schema->GetIndexConfigs();
}
std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> IndexTestHelper::GetFieldConfigs() const
{
    return _impl->schema->GetFieldConfigs();
}
const indexlibv2::config::MutableJson& IndexTestHelper::GetRuntimeSettings() const
{
    return _impl->schema->GetRuntimeSettings();
}
const std::shared_ptr<indexlibv2::index::MemIndexerParameter>& IndexTestHelper::GetMemIndexerParameter() const
{
    return _impl->memIndexerParameter;
}

const std::shared_ptr<indexlibv2::index::DiskIndexerParameter>& IndexTestHelper::GetIndexerParameter() const
{
    return _impl->indexerParameter;
}
const std::shared_ptr<indexlibv2::index::IndexReaderParameter>& IndexTestHelper::GetIndexReaderParameter() const
{
    return _impl->indexReaderParameter;
}

std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& IndexTestHelper::MutableIndexConfigs()
{
    return _impl->schema->MutableIndexConfigs();
}
std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>& IndexTestHelper::MutableFieldConfigs()
{
    return _impl->schema->MutableFieldConfigs();
}
indexlib::util::JsonMap& IndexTestHelper::MutableSettings() { return _impl->schema->MutableSettings(); }
indexlibv2::config::MutableJson& IndexTestHelper::MutableRuntimeSettings()
{
    return _impl->schema->MutableRuntimeSettings();
}
const std::shared_ptr<indexlibv2::index::DiskIndexerParameter>& IndexTestHelper::MutableIndexerParameter()
{
    return _impl->indexerParameter;
}

} // namespace indexlib::index
