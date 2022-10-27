#include "build_service/common/CheckpointList.h"

using namespace std;

namespace build_service {
namespace common {
BS_LOG_SETUP(common, CheckpointList);

void CheckpointList::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("checkpoint_list", _idSet, _idSet);
}
    
bool CheckpointList::loadFromString(const string& content) {
    try {
        FromJsonString(*this, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        stringstream ss;
        ss << "jsonize CheckpointList from [" << content
           << "] failed, exception[" << string(e.what()) << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointList::remove(const versionid_t& versionId) {
    auto it = _idSet.find(versionId);
    if (it == _idSet.end()) {
        return false;
    }
    _idSet.erase(it);
    return true;
}
    
}
}
