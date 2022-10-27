#include "indexlib/merger/test/demo_index_reduce_item.h"
#include "indexlib/merger/test/demo_indexer.h"
#include "indexlib/misc/exception.h"
#include <autil/legacy/jsonizable.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h> 

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, DemoIndexReduceItem);

const std::string DemoIndexReduceItem::mFileName = "demo_index_file";

DemoIndexReduceItem::DemoIndexReduceItem() 
{
}

DemoIndexReduceItem::~DemoIndexReduceItem() 
{
}

bool DemoIndexReduceItem::LoadIndex(const file_system::DirectoryPtr& indexDir)
{
    string fileContent;
    set<int32_t> allCatIds;
    try
    {
        for(auto res = mTaskResources.begin(); res != mTaskResources.end(); res++)
        {
            ConstString catIdStr((*res)->GetData(), (*res)->GetDataLen());
            vector<string> catIds = StringTokenizer::tokenize(catIdStr, ",");
            for(string id : catIds)
            {
                int32_t catId;
                StringUtil::fromString(id, catId);
                allCatIds.insert(catId);
            }
        }
        
        indexDir->Load(mFileName, fileContent);
        map<docid_t, std::string> tmpDataMap;
        autil::legacy::FromJsonString(tmpDataMap, fileContent);
        
        for (auto it = tmpDataMap.begin(); it != tmpDataMap.end(); it++)
        {
            ConstString tmpIdStr(it->second);
            vector<string> tmpId = StringTokenizer::tokenize(tmpIdStr, ",");
            if (tmpId.size() < 3)
            {
                continue; 
            }
            int32_t tmpCatId;
            StringUtil::fromString(tmpId[2], tmpCatId);
            if(allCatIds.find(tmpCatId) != allCatIds.end())
            {
                mDataMap.insert(make_pair(it->first, it->second));
            }
        }
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to load index file [%s], error: [%s]",
               mFileName.c_str(), e.what());
        return false;
    }
    return true;
}

bool DemoIndexReduceItem::UpdateDocId(const DocIdMap& docIdMap)
{
    std::map<docid_t, string> newDataMap;
    for (const auto& kv : mDataMap)
    {
        docid_t newDocId = docIdMap.GetNewDocId(kv.first);
        if (newDocId == INVALID_DOCID)
        {
            continue;
        }
        newDataMap[newDocId] = kv.second;
    }
    mDataMap.swap(newDataMap);
    return true;
}

bool DemoIndexReduceItem::Add(docid_t docId, const std::string& data)
{
    return mDataMap.insert(make_pair(docId, data)).second;    
}


IE_NAMESPACE_END(merger);

