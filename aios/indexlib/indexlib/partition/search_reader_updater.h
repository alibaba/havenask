#ifndef __INDEXLIB_SEARCH_READER_UPDATER_H
#define __INDEXLIB_SEARCH_READER_UPDATER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/online_partition_reader.h"

IE_NAMESPACE_BEGIN(partition);

class SearchReaderUpdater
{
public:
    SearchReaderUpdater(std::string schemaName);
    ~SearchReaderUpdater();
public:
    bool AddReaderUpdater(TableReaderContainerUpdater* updater);
    void RemoveReaderUpdater(TableReaderContainerUpdater* updater);
    bool Update(const IndexPartitionReaderPtr& reader);
    void FillVirtualAttributeConfigs(
        std::vector<config::AttributeConfigPtr>& mainAttributeConfigs,
        std::vector<config::AttributeConfigPtr>& subAttributeConfigs);
    size_t Size() { return mReaderUpdaters.size(); }
    bool HasUpdater(TableReaderContainerUpdater* updater)
    {
        for (size_t i = 0; i < mReaderUpdaters.size(); i++) 
        {
            if (updater == mReaderUpdaters[i])
            {
                return true;
            }
        }
        return false;
    }

private:
    void ConvertAttributesToNames(
        std::vector<config::AttributeConfigPtr>& mainAttributeConfigs,
        std::set<std::string>& mainAttrNames);
    void MergeAttrConfigs(
        const std::vector<config::AttributeConfigPtr>& srcAttrConfigs,
        std::set<std::string>& attrNames, 
        std::vector<config::AttributeConfigPtr>& targetAttrConfigs);
private:
    std::vector<TableReaderContainerUpdater*> mReaderUpdaters;
    mutable autil::ThreadMutex mLock;    
    std::string mSchemaName;
    bool mHasAuxTable;
    bool mIsAuxTable;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchReaderUpdater);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SEARCH_READER_UPDATER_H
