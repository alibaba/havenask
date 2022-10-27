#include "build_service/config/TaskTarget.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, TaskTarget);

TaskTarget::TaskTarget()
    : _partitionCount(0)
    , _parallelNum(0)
    , _targetTimestamp(-1)
{
}

TaskTarget::TaskTarget(const TaskTarget &other)
    : _targetName(other._targetName)
    , _partitionCount(other._partitionCount)
    , _parallelNum(other._parallelNum)
    , _description(other._description)
    , _targetTimestamp(other._targetTimestamp)
{
}

TaskTarget::TaskTarget(const string& targetName)
    : _targetName(targetName)
    , _partitionCount(0)
    , _parallelNum(0)
    , _targetTimestamp(-1)
{}

TaskTarget::~TaskTarget() {
}
    
void TaskTarget::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json){
    json.Jsonize("target_name", _targetName);
    json.Jsonize("partition_count", _partitionCount);
    json.Jsonize("parallel_num", _parallelNum);
    json.Jsonize("target_description", _description, KeyValueMap());
    json.Jsonize("target_timestamp", _targetTimestamp, _targetTimestamp);
}

bool TaskTarget::operator== (const TaskTarget& other) const {
    return _targetName == other._targetName &&
        _description == other._description &&
        _partitionCount == other._partitionCount &&
        _parallelNum == other._parallelNum &&
        _targetTimestamp == other._targetTimestamp;
}
    
}
}
