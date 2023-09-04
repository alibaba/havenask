#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/json.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/util/JsonMap.h"

namespace indexlibv2::document {
class IDocumentBatch;
}
namespace indexlibv2::index {
struct IndexerParameter;
}

namespace indexlibv2::framework {
class TabletData;
class IMemIndexer;
class IDiskIndexer;
class IIndexMerger;
class FakeMemSegment;
class FakeDiskSegment;
class IndexTaskResourceManager;
class SegmentInfo;
} // namespace indexlibv2::framework

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {

class IndexTestHelper : private autil::NoCopyable
{
public:
    IndexTestHelper();
    virtual ~IndexTestHelper();

public:
    // fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName;..
    Status MakeFieldConfigs(const std::string& fieldsDesc);
    Status MakeIndexConfig(const std::string& indexDesc);
    Status MakeSchema(const std::string& fieldsDesc, const std::string& indexDesc);
    Status Open(const std::string& localRoot, const std::string& remoteRoot);
    StatusOr<segmentid_t> Build(const std::string& docs);
    StatusOr<segmentid_t> BuildDumpingSegment(const std::string& docs);
    StatusOr<segmentid_t> BuildDiskSegment(const std::string& docs);
    StatusOr<segmentid_t> Dump();
    // docMapStr = oldSegId:oldDocId>newSegId:newDocId, oldSegId:oldDocId>:,...
    StatusOr<std::vector<segmentid_t>> Merge(std::vector<segmentid_t> sourceSegmentIds,
                                             std::vector<segmentid_t> targetSegmentIds, const std::string& docMapStr);
    Status ReOpen();
    Status ReOpen(std::vector<segmentid_t> builtDiskSegmentIds, std::vector<segmentid_t> dumpingMemSegmentIds);
    bool Query(std::string queryStr, std::string expectValue);
    void Close();

public:
    const std::string& GetTableName() const;
    const std::shared_ptr<file_system::IDirectory>& GetRootDirectory() const;
    segmentid_t GetBuildingSegmentId() const;
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> GetIndexConfigs() const;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> GetFieldConfigs() const;
    const indexlibv2::config::MutableJson& GetRuntimeSettings() const;
    const std::shared_ptr<indexlibv2::index::IndexerParameter>& GetIndexerParameter() const;

    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& MutableIndexConfigs();
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>& MutableFieldConfigs();
    indexlib::util::JsonMap& MutableSettings();
    indexlibv2::config::MutableJson& MutableRuntimeSettings();
    const std::shared_ptr<indexlibv2::index::IndexerParameter>& MutableIndexerParameter();

protected:
    virtual void DoInit() = 0;
    virtual Status DoOpen() = 0;
    virtual std::shared_ptr<indexlibv2::document::IDocumentBatch> MakeDocumentBatch(const std::string& docs) = 0;
    virtual StatusOr<std::shared_ptr<indexlibv2::config::IIndexConfig>> DoMakeIndexConfig(const std::string& indexDesc,
                                                                                          size_t indexIdx) = 0;
    virtual StatusOr<std::shared_ptr<indexlibv2::index::IMemIndexer>> CreateMemIndexer() = 0;
    // virtual StatusOr<std::shared_ptr<indexlibv2::index::IDiskIndexer>> CreateDiskIndexer() = 0;
    virtual StatusOr<std::shared_ptr<indexlibv2::index::IIndexMerger>> CreateIndexMerger() = 0;
    virtual StatusOr<std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>>
    CreateIndexTaskResourceManager(const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                                   const std::string& docMapStr) = 0;
    virtual bool DoQuery(const std::string& queryStr, const std::string& expectValue) = 0;

protected:
    indexlibv2::framework::TabletData* GetTabletData() { return _tabletData.get(); }

private:
    segmentid_t NextSegmentId();
    StatusOr<std::shared_ptr<indexlibv2::framework::FakeMemSegment>> NewMemSegment(segmentid_t segmentId);
    StatusOr<std::shared_ptr<indexlibv2::framework::FakeDiskSegment>>
    NewDiskSegment(segmentid_t segmentId, const std::shared_ptr<indexlibv2::framework::SegmentInfo>& segmentinfo);
    Status MakeTabletData();
    void PushBuildingSegment(const std::shared_ptr<indexlibv2::framework::FakeMemSegment>& buiildingMemSegment);
    void PushDumpingSegment(const std::shared_ptr<indexlibv2::framework::FakeMemSegment>& dumpingMemSegment);
    void PushBuiltSegment(const std::shared_ptr<indexlibv2::framework::FakeDiskSegment>& builtDiskSegment);
    StatusOr<indexlibv2::index::IIndexMerger::SegmentMergeInfos>
    CreateSegmentMergeInfos(const std::vector<segmentid_t>& sourceSegmentIds,
                            const std::vector<segmentid_t>& targetSegmentIds);

protected:
    std::unique_ptr<indexlibv2::index::IIndexFactory> _factory;
    std::shared_ptr<indexlibv2::framework::TabletData> _tabletData;
    std::shared_ptr<indexlibv2::config::IIndexConfig> _indexConfig;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
