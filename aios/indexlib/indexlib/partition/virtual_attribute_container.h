#ifndef __INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H
#define __INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(partition);

class VirtualAttributeContainer
{
public:
    VirtualAttributeContainer();
    ~VirtualAttributeContainer();
public:
    void UpdateUsingAttributes(const std::vector<std::pair<std::string,bool>>& attributes);
    void UpdateUnusingAttributes(const std::vector<std::pair<std::string,bool>>& attributes);
    const std::vector<std::pair<std::string,bool>>&  GetUsingAttributes();
    const std::vector<std::pair<std::string,bool>>&  GetUnusingAttributes();
    
private:
    std::vector<std::pair<std::string,bool>> mUsingAttributes;
    std::vector<std::pair<std::string,bool>> mUnusingAttributes;    
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VirtualAttributeContainer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H
