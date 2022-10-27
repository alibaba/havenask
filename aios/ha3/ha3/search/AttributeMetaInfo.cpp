#include <ha3/search/AttributeMetaInfo.h>

using namespace std;
IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AttributeMetaInfo);

AttributeMetaInfo::AttributeMetaInfo(
        const string &attrName, 
        AttributeType attrType, 
        const IndexPartitionReaderPtr &indexPartReaderPtr)
    : _attrName(attrName)
    , _attrType(attrType)
    , _indexPartReaderPtr(indexPartReaderPtr)
{
}

AttributeMetaInfo::~AttributeMetaInfo() { 
}

END_HA3_NAMESPACE(search);

