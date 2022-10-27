#ifndef ISEARCH_BS_COLLECTRESULT_H
#define ISEARCH_BS_COLLECTRESULT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/StringUtil.h>
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace reader {

struct CollectResult : public autil::legacy::Jsonizable {
    uint32_t _globalId;
    uint16_t _rangeId;
    std::string _fileName;

    CollectResult()
        : _globalId(0)
        , _rangeId(0)
    {}

    CollectResult(const std::string &fileName)
        : _globalId(0)
        , _rangeId(0)
        , _fileName(fileName)
    {}

    CollectResult(uint32_t globalId, const std::string &fileName)
        : _globalId(globalId)
        , _rangeId(0)
        , _fileName(fileName)
    {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("global_id", _globalId);
        json.Jsonize("range_id", _rangeId);
        json.Jsonize("file_name", _fileName);
    }
    
    bool operator==(const CollectResult &other) const {
        return _globalId == other._globalId &&
            _rangeId == other._rangeId &&
            _fileName == other._fileName;
    }
    bool operator!=(const CollectResult &other) const {
        return !(*this == other);
    }
    std::string toString() const {
        return _fileName + ":" + autil::StringUtil::toString(_rangeId)
            + ":" + autil::StringUtil::toString(_globalId);
    }
};
typedef std::vector<CollectResult> CollectResults;
typedef std::map<std::string, CollectResults> DescriptionToCollectResultsMap;
}
}

#endif //ISEARCH_BS_COLLECTRESULT_H
