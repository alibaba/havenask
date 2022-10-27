#ifndef ISEARCH_INDEXLIB_LOCATOR_H
#define ISEARCH_INDEXLIB_LOCATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

// move from build_service
class IndexLocator
{
public:
    explicit IndexLocator(int64_t offset = -1)
        : _src(0)
        , _offset(offset)
    {}
    
    explicit IndexLocator(uint64_t src, int64_t offset)
        : _src(src)
        , _offset(offset)
    {}
    
    IndexLocator(const IndexLocator &other)
        : _src(other._src)
        , _offset(other._offset)
    {}
    
    ~IndexLocator() {}
    
public:
    friend bool operator==(const IndexLocator &lft, const IndexLocator &rht) {
        return lft._offset == rht._offset && lft._src == rht._src;
    }
    
    friend bool operator!=(const IndexLocator &lft, const IndexLocator &rht) {
        return !(lft == rht);
    }

    uint64_t getSrc() const {
        return _src;
    }
    void setSrc(uint64_t src) {
        _src = src;
    }
    int64_t getOffset() const {
        return _offset;
    }
    void setOffset(int64_t offset) {
        _offset = offset;
    }

    // WARNING: only use this to serialize/deserialize
    std::string toString() const;

    template <typename StringType>
    bool fromString(const StringType &str) {
        if (str.size() != size()) {
            return false;
        }
        char *data = (char *)str.data();
        _src = *(uint64_t *)data;
        data += sizeof(_src);
        _offset = *(int64_t *)data;
        return true;
    }

    // use this to LOG
    std::string toDebugString() const;

    size_t size() const {
        return sizeof(_src) + sizeof(_offset);
    }
    
private:
    uint64_t _src;
    int64_t _offset;
};

extern const IndexLocator INVALID_INDEX_LOCATOR;

IE_NAMESPACE_END(common);

#endif //ISEARCH_INDEXLIB_LOCATOR_H
