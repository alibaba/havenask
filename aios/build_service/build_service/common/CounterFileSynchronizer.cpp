#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/misc/exception.h>

using namespace std;
using namespace build_service::util;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace common {
BS_LOG_SETUP(common, CounterFileSynchronizer);

CounterFileSynchronizer::CounterFileSynchronizer() {
}

CounterFileSynchronizer::~CounterFileSynchronizer() {
    stopSync();
}

CounterMapPtr CounterFileSynchronizer::loadCounterMap(
            const std::string &counterFilePath, bool &fileExist)
{
    fileExist = false;
    if (counterFilePath.empty()) {
        BS_LOG(ERROR, "invalid counter file path[%s]", counterFilePath.c_str());
        return CounterMapPtr();
    }

    auto fileExistPredicate = std::bind((bool(*)(const string &, bool &))&FileUtil::isExist,
            counterFilePath, std::ref(fileExist));

    if (!doWithRetry(fileExistPredicate, 10, 5)) {
        BS_LOG(ERROR, "check file path[%s] existence failed", counterFilePath.c_str());
        fileExist = false;
        return CounterMapPtr();
    }

    CounterMapPtr counterMap(new CounterMap());
    if (!fileExist) {
        return counterMap;
    }

    string counterJsonStr;
    auto readFilePredicate = std::bind((bool(*)(const string &, string &))&FileUtil::readFile,
            counterFilePath, std::ref(counterJsonStr));

    if (!doWithRetry(readFilePredicate, 10, 5)) {
        BS_LOG(ERROR, "read counterMap from [%s] failed.", counterFilePath.c_str());
        return CounterMapPtr(); 
    }

    try {
        counterMap->FromJsonString(counterJsonStr);
    } catch (const ExceptionBase& e) {
        BS_LOG(WARN, "deserialize counterMap json[%s] for [%s] failed",
               counterJsonStr.c_str(), counterFilePath.c_str());
        fileExist = false;
        return CounterMapPtr(new CounterMap());
    }

    return counterMap;
}

bool CounterFileSynchronizer::init(const CounterMapPtr& counterMap,
                                   const string& counterFilePath)
{
    if (!CounterSynchronizer::init(counterMap)) {
        return false;
    }
    _counterFilePath = counterFilePath;
    return true;
}

bool CounterFileSynchronizer::sync() const {
    if (!_counterMap) {
        BS_LOG(ERROR, "counterMap is NULL");
        return false;
    }
    string counterJsonStr = _counterMap->ToJsonString();
    if (!FileUtil::writeFile(_counterFilePath, counterJsonStr)) {
        BS_LOG(ERROR, "serialize counterMap[%s] to file [%s] failed",
                       counterJsonStr.c_str(), _counterFilePath.c_str());
        return false;
    }
    return true;
}

}
}
