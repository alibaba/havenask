#include "indexlib/partition/search_reader_updater.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SearchReaderUpdater);

SearchReaderUpdater::SearchReaderUpdater(string schemaName)
    : mSchemaName(schemaName)
    , mHasAuxTable(false)
    , mIsAuxTable(false)
{    
}

SearchReaderUpdater::~SearchReaderUpdater() 
{
}

bool SearchReaderUpdater::AddReaderUpdater(TableReaderContainerUpdater* updater)
{
    ScopedLock lock(mLock);
    if (!mIsAuxTable)
    {
        mIsAuxTable = updater->IsAuxTable(mSchemaName);
    }

    if (!mHasAuxTable)
    {
        mHasAuxTable = updater->HasAuxTable(mSchemaName);
    }

    if (mIsAuxTable && mHasAuxTable)
    {
        IE_LOG(ERROR, "table [%s] can not has auxTable and be an auxTable", 
               mSchemaName.c_str());
        return false;
    }
    
    for (auto updaterPtr : mReaderUpdaters)
    {
        if (updaterPtr == updater)
        {
            return true;
        }
    }
    mReaderUpdaters.push_back(updater);
    return true;
}


void SearchReaderUpdater::RemoveReaderUpdater(TableReaderContainerUpdater* updater)
{
    ScopedLock lock(mLock);
    size_t removeIdx = mReaderUpdaters.size();
    for (size_t i = 0; i < mReaderUpdaters.size(); i ++)
    {
        if (mReaderUpdaters[i] == updater)
        {
            removeIdx = i;
        }
    }
    if (removeIdx < mReaderUpdaters.size())
    {
        mReaderUpdaters.erase(mReaderUpdaters.begin() + removeIdx);
    }
}

bool SearchReaderUpdater::Update(
    const IndexPartitionReaderPtr& reader)
{
    ScopedLock lock(mLock);
    bool ret = true;
    for (auto updater : mReaderUpdaters)
    {
        if (!updater->Update(reader))
        {
            ret = false;
        }
    }
    return ret;
}

void SearchReaderUpdater::ConvertAttributesToNames(
    vector<AttributeConfigPtr>& mainAttributeConfigs,
    set<string>& mainAttrNames)
{
    for (auto attrConfig : mainAttributeConfigs)
    {
        mainAttrNames.insert(attrConfig->GetAttrName());
    }
}

void SearchReaderUpdater::MergeAttrConfigs(
    const vector<AttributeConfigPtr>& srcAttrConfigs,
    set<string>& attrNames, vector<AttributeConfigPtr>& targetAttrConfigs)
{
    for (size_t i = 0; i < srcAttrConfigs.size(); i++)
    {
        string attrName = srcAttrConfigs[i]->GetAttrName();
        if (attrNames.find(attrName) == attrNames.end())
        {
            attrNames.insert(attrName);
            targetAttrConfigs.push_back(srcAttrConfigs[i]);
        }
    }
}

void SearchReaderUpdater::FillVirtualAttributeConfigs(
    vector<AttributeConfigPtr>& mainAttributeConfigs,
    vector<AttributeConfigPtr>& subAttributeConfigs)
{
    ScopedLock lock(mLock);
    set<string> mainAttrNames;
    ConvertAttributesToNames(mainAttributeConfigs, mainAttrNames);
    set<string> subAttrNames;
    ConvertAttributesToNames(subAttributeConfigs, subAttrNames);
    for (auto readerUpdater : mReaderUpdaters)
    {
        vector<AttributeConfigPtr> mainAttributes;
        vector<AttributeConfigPtr> subAttributes;
        readerUpdater->FillVirtualAttributeConfigs(
            mSchemaName, mainAttributes, subAttributes);
        MergeAttrConfigs(mainAttributes, mainAttrNames, mainAttributeConfigs);
        MergeAttrConfigs(subAttributes, subAttrNames, subAttributeConfigs);
    }
}

IE_NAMESPACE_END(partition);

