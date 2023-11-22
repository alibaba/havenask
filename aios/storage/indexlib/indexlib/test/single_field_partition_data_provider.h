#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/online_partition.h"

namespace indexlib { namespace test {

enum SFP_BuildType { SFP_REALTIME, SFP_OFFLINE };

enum SFP_IndexType { SFP_PK_INDEX, SFP_PK_128_INDEX, SFP_TRIE, SFP_INDEX, SFP_ATTRIBUTE, SFP_SUMMARY, SFP_UNKNOW };

class SingleFieldPartitionDataProvider
{
public:
    static const std::string INDEX_NAME_TAG;

public:
    // SingleFieldPartitionDataProvider(bool spatialField = false);
    // SingleFieldPartitionDataProvider(const config::IndexPartitionOptions& options, bool spatialField = false);
    SingleFieldPartitionDataProvider();
    SingleFieldPartitionDataProvider(const config::IndexPartitionOptions& options);
    ~SingleFieldPartitionDataProvider() {}

public:
    // fieldTypeStr = fieldType[:isMultiValue:isUpdatable:CompressType]
    void Init(const std::string& rootDir, const std::string& fieldTypeStr, SFP_IndexType type, bool enableNull = false);

    // docStrings:    [segDocStr#segDocStr#...]
    // segDocStr:     [docFieldValue,docFieldValue,...]
    // docFieldValue: [addDoc|delDoc|updateDoc]
    //    addDoc:     add:fieldValue or fieldValue
    //    delDoc:     delete:pkValue
    //    updateDoc:  update_field:pkValue:fieldValue (pkValue is docId)
    bool Build(const std::string& docStrings, SFP_BuildType buildType);

    // mergedSegmentIdsStr : 0,1,3
    void Merge(const std::string& mergedSegmentIdsStr);

    index_base::PartitionDataPtr GetPartitionData();
    config::IndexConfigPtr GetIndexConfig();
    config::AttributeConfigPtr GetAttributeConfig();
    partition::OnlinePartitionPtr GetOnlinePartition() { return mOnlinePartition; }
    // TODO: getAttributeConfig, getIndexConfig, getPkConfig

    const config::IndexPartitionSchemaPtr& GetSchema() const { return mSchema; }

    std::vector<document::NormalDocumentPtr> CreateDocuments(const std::string& docString);
    config::IndexPartitionOptions GetOptions() const { return mOptions; }

public:
    void EnableSpatialField() { mSpatialField = true; }

private:
    void InitSchema(const std::string& fieldTypeStr, SFP_IndexType type);

    partition::IndexBuilderPtr CreateIndexBuilder(SFP_BuildType buildType);
    void BuildOneSegmentData(const partition::IndexBuilderPtr& builder, const std::string& docString);

    std::string NormalizeOneDocString(const std::string& oneDocStr);
    std::string CreateAddDocumentStr(const std::string& fieldValue);
    std::string CreateDeleteDocumentStr(const std::string& fieldValue);
    std::string CreateUpdateDocumentStr(const std::string& pkValue, const std::string& fieldValue);
    bool IsPrimaryKey() const;
    void GetEntryTableId(const std::string& path, int32_t& id) const;

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    partition::OnlinePartitionPtr mOnlinePartition;
    SFP_IndexType mType;
    uint64_t mDocCount;
    uint64_t mAddDocCount;
    bool mSpatialField = false;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldPartitionDataProvider);
}} // namespace indexlib::test
