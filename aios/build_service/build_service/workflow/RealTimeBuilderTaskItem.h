#ifndef ISEARCH_BS_REALTIMEBUILDERTASKITEM_H
#define ISEARCH_BS_REALTIMEBUILDERTASKITEM_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/util/task_item.h>

namespace build_service {
namespace workflow {

class RealtimeBuilderImplBase;
    
class RealTimeBuilderTaskItem : public IE_NAMESPACE(util)::TaskItem
{
public:
    RealTimeBuilderTaskItem(RealtimeBuilderImplBase* rtBuilderImpl);
    ~RealTimeBuilderTaskItem();

public:
    void Run() override;
    
private:
    RealTimeBuilderTaskItem(const RealTimeBuilderTaskItem &);
    RealTimeBuilderTaskItem& operator=(const RealTimeBuilderTaskItem &);
private:
    RealtimeBuilderImplBase* _rtBuilderImpl;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealTimeBuilderTaskItem);

}
}

#endif //ISEARCH_BS_REALTIMEBUILDERTASKITEM_H
