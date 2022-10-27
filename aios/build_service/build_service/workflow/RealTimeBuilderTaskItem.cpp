#include "build_service/workflow/RealTimeBuilderTaskItem.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"

using namespace std;

namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, RealTimeBuilderTaskItem);

RealTimeBuilderTaskItem::RealTimeBuilderTaskItem(RealtimeBuilderImplBase* rtBuilderImpl)
    : _rtBuilderImpl(rtBuilderImpl)
{
}

RealTimeBuilderTaskItem::~RealTimeBuilderTaskItem() {
}

void RealTimeBuilderTaskItem::Run()
{
    assert(_rtBuilderImpl);
    _rtBuilderImpl->executeBuildControlTask();
}
    
}
}
