#ifndef ISEARCH_COUNTEDATTRIBUTEITERATOR_H
#define ISEARCH_COUNTEDATTRIBUTEITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/attribute/accessor/attribute_iterator_base.h>
#include <autil/StringUtil.h>

IE_NAMESPACE_BEGIN(index);

template <typename T>
class CountedAttributeIterator : public AttributeIteratorBase
{
public:
    CountedAttributeIterator(const std::string &attrValues) {
        autil::StringUtil::fromString<T>(attrValues, _values, ",");
    }
public:
    /* override */ void Reset() { }

    inline bool Seek(docid_t docId, T& value) {
        if (_seekedCountMap.find(docId) == _seekedCountMap.end()) {
            _seekedCountMap[docId] = 0;
        }
        _seekedCountMap[docId]++;
        if (docId >= (docid_t)_values.size()) {
            return false;
        }
        value = _values[docId];
        return true;
    }
public:
    uint32_t getSeekedCount(docid_t docId) {
        if (_seekedCountMap.find(docId) == _seekedCountMap.end()) {
            return 0;
        }
        return _seekedCountMap[docId];
    }
private:
    std::vector<T> _values;
    std::map<docid_t, uint32_t> _seekedCountMap;
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_COUNTEDATTRIBUTEITERATOR_H
