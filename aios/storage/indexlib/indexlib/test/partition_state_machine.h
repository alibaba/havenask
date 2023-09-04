#ifndef __INDEXLIB_PARTITION_STATE_MACHINE_H
#define __INDEXLIB_PARTITION_STATE_MACHINE_H

#include <memory>
#include <unordered_map>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/partition_metrics.h"
#include "indexlib/test/result.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace test {

enum PsmEvent {
    // Build Group
    PE_BUILD_FULL = 0x1,
    PE_BUILD_INC = 0x2,
    PE_BUILD_RT = 0x4,
    PE_BUILD_MASK = 0xF,
    // Merge Group
    PE_MERGE = 0x10,
    PE_MERGE_MASK = 0xF0,
    // Reopen Group
    PE_REOPEN = 0x100,
    PE_REOPEN_FORCE = 0x200,
    PE_REOPEN_MASK = 0xF00,
    // Metrics Check
    PE_METRICS = 0x1000,
    // DUMP EVENT
    PE_DUMP_SEGMENT = 0x2000,
    // QUERY_NO_CACHE
    PE_QUERY_NO_CACHE = 0x4000,
    // QUERY_ONLY_CACHE
    PE_QUERY_ONLY_CACHE = 0x8000,
    // Build Options
    PE_MEM_LIMIT = 0x10000,
    // Combination Group
    BUILD_FULL_NO_REOPEN = PE_BUILD_FULL | PE_MERGE,
    BUILD_INC_NO_REOPEN = PE_BUILD_INC | PE_MERGE,
    BUILD_FULL = PE_BUILD_FULL | PE_MERGE | PE_REOPEN,
    BUILD_INC = PE_BUILD_INC | PE_MERGE | PE_REOPEN,
    BUILD_RT = PE_BUILD_RT,
    BUILD_RT_MEM_LIMIT = PE_BUILD_RT | PE_MEM_LIMIT,
    BUILD_RT_SEGMENT = PE_BUILD_RT | PE_DUMP_SEGMENT,
    BUILD_INC_NO_MERGE = PE_BUILD_INC | PE_REOPEN,
    BUILD_FULL_NO_MERGE = PE_BUILD_FULL | PE_REOPEN,
    BUILD_INC_NO_MERGE_FORCE_REOPEN = PE_BUILD_INC | PE_REOPEN_FORCE,
    FORCE_REOPEN = PE_REOPEN_FORCE,
    QUERY = 0x0,
    QUERY_NO_CACHE = PE_QUERY_NO_CACHE,
    QUERY_ONLY_CACHE = PE_QUERY_ONLY_CACHE,
    BUILD_FULL_NO_MERGE_DUMP = BUILD_FULL_NO_MERGE | PE_DUMP_SEGMENT,
};

class PartitionStateMachine
{
public:
    PartitionStateMachine(
        bool buildSerializeDoc = false,
        const partition::IndexPartitionResource& partitionResource = partition::IndexPartitionResource(),
        const partition::DumpSegmentContainerPtr& dumpSegmentContainer =
            partition::DumpSegmentContainerPtr(new partition::DumpSegmentContainer));

    ~PartitionStateMachine();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, config::IndexPartitionOptions options, std::string root,
              std::string partitionName = "psm", std::string secondRoot = "", uint32_t branchId = 0,
              std::string globalCacheStr = "");

    void SetTermPayloadMap(const std::unordered_map<std::string, termpayload_t>& termMap) { mTermPayloadMap = termMap; }

    void SetSchema(const config::IndexPartitionSchemaPtr& schema) { mSchema = schema; }

    config::IndexPartitionSchemaPtr GetSchema() { return mSchema; }
    // current PsmOptions
    // set legacyKvDocFormat = true , will create kkv document in legacy format
    // set defaultRegion = $regionName, will build and query within $regionName
    // set resultCheckTYpe = ORDERED, will check result by order
    // set resultCheckTYpe = UNORDERED, will check result ignored order
    // set documentFormat = binary, will treat data as serialized Documents
    bool SetPsmOption(const std::string& optionName, const std::string& optionValue)
    {
        return mPsmOptions.insert(make_pair(optionName, optionValue)).second;
    }

    void SetCurrentTimestamp(int64_t currentTs) { mCurrentTimestamp = currentTs; }

    bool Transfer(PsmEvent event, const std::string& data, const std::string& queryString,
                  const std::string& resultString);

    const partition::IndexPartitionPtr& GetIndexPartition() const { return mOnlinePartition; }
    void CleanResources();

    file_system::DirectoryPtr GetRootDirectory() const
    {
        return file_system::Directory::Get(mOnlinePartition->GetFileSystem());
    }

    file_system::IFileSystemPtr GetFileSystem() const { return mOnlinePartition->GetFileSystem(); }

    bool ExecuteEvent(PsmEvent event, const std::string& data);
    ResultPtr Search(const std::string& queryString, TableSearchCacheType searchCache);

    PartitionMetricsPtr GetMetrics();
    util::MetricProviderPtr GetMetricsProvider() const { return mMetricProvider; }

    void SetMergeConfig(const config::MergeConfig& mergeConfig)
    {
        mOptions.GetOfflineConfig().mergeConfig = mergeConfig;
    }

    void SetMergeTTL(int64_t ttl) { mOptions.GetBuildConfig(false).ttl = ttl; }

    void DisableTestQuickExit() { unsetenv("TEST_QUICK_EXIT"); }

    void SetOngoingModifyOperations(const std::vector<schema_opid_t>& opIds)
    {
        mOptions.SetOngoingModifyOperationIds(opIds);
    }

    bool CreateOnlinePartition(versionid_t targetVersionId);
    partition::IndexPartitionResource& GetIndexPartitionResource() { return mPartitionResource; }

    static std::string SerializeDocuments(const std::vector<document::DocumentPtr>& inDocs);
    static std::string SerializeDocuments(const std::vector<document::NormalDocumentPtr>& inDocs);

private:
    partition::IndexBuilderPtr CreateIndexBuilder(PsmEvent event);

    // data fomat: docStr;docStr;...##controlCmd;controlCmd;
    // docStr: cmd=add,field1=xx,field2=xx
    // controlCmd: stopTs=xx
    int64_t BatchBuild(const partition::IndexBuilderPtr& builder, const std::string& data, bool needDump = true,
                       bool needDumpSegment = false);

    void Build(PsmEvent event, const std::string& data);

    bool NeedBuild(PsmEvent event) { return event & PE_BUILD_MASK; }
    bool NeedCheckMetrics(PsmEvent event) { return event == PE_METRICS; }
    bool DoBuildEvent(PsmEvent event, const std::string& data);
    bool NeedReOpen(PsmEvent event) { return event & PE_REOPEN_MASK; }
    bool RefreshOnlinePartition(PsmEvent event, const std::string& data);
    ResultPtr SearchKKV(const std::string& queryString, TableSearchCacheType searchCache);
    ResultPtr SearchKV(const std::string& queryString, TableSearchCacheType searchCache);
    ResultPtr SearchCustomTable(const std::string& queryString);

    bool NeedUnorderCheck() const;
    TableSearchCacheType GetSearchCacheType(PsmEvent event) const;

    std::vector<document::DocumentPtr> DeserializeDocuments(const std::string& docBinaryStr);

    int64_t GetNumber(const std::string str) const;
    void UpdateRtBuildStatus();
    void ValidateLatestIndexVersion();

public:
    document::DocumentPtr CreateDocument(const std::string& docString);
    std::vector<document::DocumentPtr> CreateDocuments(const std::string& docString);

    static void PrepareTokenizedField(const std::string& fieldName, const config::FieldConfigPtr& fieldConfig,
                                      const config::IndexSchemaPtr& indexSchema, const test::RawDocumentPtr& rawDoc,
                                      const document::TokenizeDocumentPtr& tokenDoc);

public:
    static std::string BUILD_CONTROL_SEPARATOR;

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRoot;
    std::string mSecondaryRootPath;

    util::MetricProviderPtr mMetricProvider;
    partition::IndexPartitionResource mPartitionResource;
    partition::IndexPartitionPtr mOnlinePartition;
    bool mBuildSerializedDoc;
    bool mIsSuspendRtBuild;
    std::unordered_map<std::string, termpayload_t> mTermPayloadMap;
    int64_t mCurrentTimestamp;
    partition::DumpSegmentContainerPtr mDumpSegmentContainer;
    std::map<std::string, std::string> mPsmOptions;
    document::DocumentFactoryWrapperPtr mDocFactory;
    document::DocumentParserPtr mDocParser;
    document::DocumentRewriterPtr mHashIdDocumentRewriter;
    autil::mem_pool::Pool mPool;
    uint32_t mFsBranchId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionStateMachine);
}} // namespace indexlib::test

#endif //__INDEXLIB_PARTITION_STATE_MACHINE_H
