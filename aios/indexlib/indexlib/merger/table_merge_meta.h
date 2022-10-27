#ifndef __INDEXLIB_TABLE_MERGE_META_H
#define __INDEXLIB_TABLE_MERGE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/merge_task_dispatcher.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/merger/index_merge_meta.h"

DECLARE_REFERENCE_CLASS(table, TableMergePlan);
DECLARE_REFERENCE_CLASS(table, TableMergePlanMeta);
DECLARE_REFERENCE_CLASS(table, TableMergePlanResource);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(merger);

// TODO: inherites from MergeMeta
class TableMergeMeta : public IndexMergeMeta
{
public:
    TableMergeMeta(const table::MergePolicyPtr& mergePolicy);
    ~TableMergeMeta();

public:
    static const std::string MERGE_META_VERSION;    
    static const std::string MERGE_META_BINARY_VERSION;
    static const std::string TASK_EXECUTE_META_FILE;
    static const std::string MERGE_PLAN_DIR_PREFIX;
    static const std::string MERGE_PLAN_RESOURCE_DIR_PREFIX;
    static const std::string MERGE_PLAN_FILE;
    static const std::string MERGE_PLAN_META_FILE;
    static const std::string MERGE_PLAN_TASK_DESCRIPTION_FILE;
    
public:
    void Store(const std::string &rootPath) const override;
    bool Load(const std::string &rootPath) override
    {
        return Load(rootPath, true);
    }
    bool LoadBasicInfo(const std::string &rootPath) override
    {
        return Load(rootPath, false);
    }
    size_t GetMergePlanCount() const override
    {
        return mergePlans.size();
    }
    std::vector<segmentid_t> GetTargetSegmentIds(size_t planIdx) const override;

    int64_t EstimateMemoryUse() const override;
    table::MergePolicyPtr GetMergePolicy() const
    {
        return mMergePolicy;
    }

private:
    bool Load(const std::string &rootPath, bool loadFullMeta);
    bool StoreSingleMergePlan(size_t planIdx, const file_system::DirectoryPtr& planDir) const;
    bool LoadSingleMergePlan(size_t planIdx, const file_system::DirectoryPtr& planDir,
                             bool loadResource); 
    bool LoadMergeMetaVersion(const std::string &path, std::string &mergeMetaBinaryVersion);
    
public:
    // for each mergePlan
    std::vector<table::TableMergePlanPtr> mergePlans;
    std::vector<table::TableMergePlanMetaPtr> mergePlanMetas;
    std::vector<table::TableMergePlanResourcePtr> mergePlanResources;
    std::vector<table::MergeTaskDescriptions> mergeTaskDescriptions;
    // for each merge instance
    std::vector<table::TaskExecuteMetas> instanceExecMetas;
    
private:
    table::MergePolicyPtr mMergePolicy;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergeMeta);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TABLE_MERGE_META_H
