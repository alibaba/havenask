#include "indexlib/partition/virtual_attribute_container.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, VirtualAttributeContainer);

VirtualAttributeContainer::VirtualAttributeContainer() 
{
}

VirtualAttributeContainer::~VirtualAttributeContainer() 
{
}

void VirtualAttributeContainer::UpdateUsingAttributes(const vector<pair<string,bool>>& attributes)
{
    for (size_t i = 0; i < mUsingAttributes.size(); i ++)
    {
        bool exist = false;
        for (size_t j = 0; j < attributes.size(); j ++)
        {
            if (attributes[j] == mUsingAttributes[i])
            {
                exist = true;
                break;
            }
        }
        if (!exist)
        {
            mUnusingAttributes.push_back(mUsingAttributes[i]);
        }
    }
    mUsingAttributes = attributes;
}

void VirtualAttributeContainer::UpdateUnusingAttributes(const vector<pair<string,bool>>& attributes)
{
    mUnusingAttributes = attributes;
}

const vector<pair<string,bool>>& VirtualAttributeContainer::GetUsingAttributes()
{
    return mUsingAttributes;
}

const vector<pair<string,bool>>& VirtualAttributeContainer::GetUnusingAttributes()
{
    return mUnusingAttributes;
}

IE_NAMESPACE_END(partition);

