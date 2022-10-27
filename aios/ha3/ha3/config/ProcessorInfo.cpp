#include <ha3/config/ProcessorInfo.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(qrs, ProcessorInfo);

ProcessorInfo::ProcessorInfo() {
}

ProcessorInfo::ProcessorInfo(string processorName, string moduleName)
{
    _processorName = processorName;
    _moduleName = moduleName;
}

ProcessorInfo::~ProcessorInfo() {
}

string ProcessorInfo::getProcessorName() const {
    return _processorName;
}

void ProcessorInfo::setProcessorName(const string &processorName) {
    _processorName = processorName;
}

string ProcessorInfo::getModuleName() const {
    return _moduleName;
}

void ProcessorInfo::setModuleName(const string &moduleName) {
    _moduleName = moduleName;
}

string ProcessorInfo::getParam(const string &key) const {
    KeyValueMap::const_iterator it = _params.find(key);
    if (it != _params.end()) {
        return it->second;
    } else {
        return "";
    }
}

void ProcessorInfo::addParam(const string &key, const string &value) {
    KeyValueMap::const_iterator it = _params.find(key);
    if (it == _params.end()) {
        _params.insert(make_pair(key, value));
    } else {
        HA3_LOG(WARN, "already exist this key[%s], old_value[%s], insert_value[%s]",
            key.c_str(), it->second.c_str(), value.c_str());
    }
}

const KeyValueMap& ProcessorInfo::getParams() const {
    return _params;
}

void ProcessorInfo::setParams(const KeyValueMap &params) {
    _params = params;
}

void ProcessorInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("module_name", _moduleName, _moduleName);
    json.Jsonize("parameters", _params, _params);
    json.Jsonize("processor_name", _processorName, _processorName);
    if (json.GetMode() == FROM_JSON) {
        // compatible with document processor
        string className;
        json.Jsonize("class_name", className, "");
        if (!className.empty()) {
            _processorName = className;
        }
    }
}

bool ProcessorInfo::operator==(const ProcessorInfo &other) const {
    if (&other == this) {
        return true;
    }
    return _processorName == other._processorName
        && _moduleName == other._moduleName
        && _params == other._params;
}

END_HA3_NAMESPACE(config);
