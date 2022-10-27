#ifndef ISEARCH_BS_DATADESCRIPTIONS_H
#define ISEARCH_BS_DATADESCRIPTIONS_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/DataDescription.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace proto {

class DataDescriptions
{
public:
    DataDescriptions();
    ~DataDescriptions();
public:    
    DataDescription &operator[](size_t n) {
        assert(n < _dataDescriptions.size());
        return _dataDescriptions[n];
    }
    const DataDescription &operator[](size_t n) const {
        assert(n < _dataDescriptions.size());
        return _dataDescriptions[n];
    }
    void push_back(const DataDescription &x) {
        _dataDescriptions.push_back(x);
    }
    void pop_back() {
        _dataDescriptions.pop_back();
    }
    bool empty() const {
        return _dataDescriptions.empty();
    }
    size_t size() const {
        return _dataDescriptions.size();
    }
    bool operator==(const DataDescriptions &other) const {
        return toVector() == other.toVector();
    }
    void clear() {
        _dataDescriptions.clear();
    }
public:
    // for jsonize, autil Jsonizable not support jsonize array on root
    const std::vector<DataDescription> &toVector() const {
        return _dataDescriptions;
    }
    std::vector<DataDescription> &toVector() {
        return _dataDescriptions;
    }
public:
    bool validate() const;
private:
    std::vector<DataDescription> _dataDescriptions;
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_DATADESCRIPTIONS_H
