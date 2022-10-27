#include "indexlib/index/normal/attribute/attribute_update_info.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_define.h"

using namespace autil::legacy::json;
using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeUpdateInfo);

void AttributeUpdateInfo::Jsonize(Jsonizable::JsonWrapper& json)
{
    SegmentUpdateInfoVec segUpdateInfos;
    if (json.GetMode() == TO_JSON)
    {
        Iterator iter = CreateIterator();
        while(iter.HasNext())
        {
            segUpdateInfos.push_back(iter.Next());
        }
        json.Jsonize("attribute_update_info", segUpdateInfos);
    }
    else
    {
        json.Jsonize("attribute_update_info", segUpdateInfos, segUpdateInfos);
        for (size_t i = 0; i < segUpdateInfos.size(); i++)
        {
            if (!Add(segUpdateInfos[i]))
            {
                INDEXLIB_FATAL_ERROR(Duplicate,
                                     "duplicated updateSegId[%d] "
                                     "in attribute update info",
                                     segUpdateInfos[i].updateSegId);
            }
        }
    }
}

void AttributeUpdateInfo::Load(const DirectoryPtr& directory)
{
    assert(directory);
    string content;
    directory->Load(ATTRIBUTE_UPDATE_INFO_FILE_NAME, content);
    FromJsonString(*this, content);
}


bool AttributeUpdateInfo::operator == (const AttributeUpdateInfo& other) const
{
    if (mUpdateMap.size() != other.mUpdateMap.size())
    {
        return false;
    }

    Iterator iter1 = CreateIterator();
    Iterator iter2 = other.CreateIterator();

    while(iter1.HasNext())
    {
        assert(iter2.HasNext());
        if (iter1.Next() != iter2.Next())
        {
            return false;
        }
    }
    return true;
}

IE_NAMESPACE_END(index);

