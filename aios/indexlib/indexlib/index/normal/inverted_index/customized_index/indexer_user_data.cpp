#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"

using namespace std;

namespace heavenask { namespace indexlib {

IndexerUserData::IndexerUserData() 
{
}

IndexerUserData::~IndexerUserData() 
{
}

bool IndexerUserData::init(const vector<IndexerSegmentData>& segDatas)
{
    return true;
}

size_t IndexerUserData::estimateInitMemUse(
        const vector<IndexerSegmentData>& segDatas) const
{
    return 0;
}

void IndexerUserData::setData(const string& key, const string& value)
{
    mDataMap[key] = value;
}

bool IndexerUserData::getData(const string& key, string& value) const
{
    auto iter = mDataMap.find(key);
    if (iter != mDataMap.end()) {
        value = iter->second;
        return true;
    }
    return false;
}

}
}

