#include <autil/StringUtil.h>
#include "build_service/common/TaskIdentifier.h"

using namespace std;
using namespace autil;

namespace build_service {
namespace common {
BS_LOG_SETUP(common, TaskIdentifier);

const std::string TaskIdentifier::FIELD_SEPARATOR = "-";
const std::string TaskIdentifier::FIELDNAME_CLUSTERNAME = "clusterName";
const std::string TaskIdentifier::FIELDNAME_TASKNAME = "taskName";
const std::string TaskIdentifier::FIELDNAME_SEPARATOR = "=";

TaskIdentifier::TaskIdentifier()
{
}

TaskIdentifier::~TaskIdentifier() {
}

void TaskIdentifier::setValue(const string& key, const string& value)
{
    _kvMap[key] = value;
}

bool TaskIdentifier::getValue(const string& key, string& value) const
{
    KeyValueMap::const_iterator iter = _kvMap.find(key);
    if (iter != _kvMap.end()) {
        value = iter->second;
        return true;
    }
    return false;
}

bool TaskIdentifier::fromString(const string &content)
{
    vector<string> strVec = StringUtil::split(content, FIELD_SEPARATOR, false);
    if (strVec.empty()) {
        return false;
    }
    for (size_t i = 0; i < strVec.size(); i++) {
        vector<string> kvStr = StringUtil::split(strVec[i], FIELDNAME_SEPARATOR, false);
        if (kvStr.size() != 2) {
            return false;
        }
        if (i == 0) {
            if (kvStr[0] != "taskId") {
                return false;
            }
            _taskId = kvStr[1];
            continue;
        }
        _kvMap[kvStr[0]] = kvStr[1];
    }
    return true;
}

string TaskIdentifier::toString() const
{
    vector<string> strInfos;
    strInfos.push_back(string("taskId") + FIELDNAME_SEPARATOR + _taskId);
    for (auto &kv : _kvMap) {
        strInfos.push_back(kv.first + FIELDNAME_SEPARATOR + kv.second);
    }
    return StringUtil::toString(strInfos, FIELD_SEPARATOR);
}

}
}
