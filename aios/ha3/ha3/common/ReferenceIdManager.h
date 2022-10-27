#ifndef ISEARCH_HA3_REFERENCEIDMANAGER_H
#define ISEARCH_HA3_REFERENCEIDMANAGER_H

BEGIN_HA3_NAMESPACE(common);

class ReferenceIdManager
{
    /**
     * in searcher, we have 32 bit doc flag(see DocIdentifier), we use first
     * bit to present whether the match doc has been deleted, and we can use
     * another 31 bit to present whether an AttributeExpression had been
     * evaluated in this match doc.
     */
public:
    static const uint32_t MAX_REFERENCE_ID = 31;
    static const uint32_t INVALID_REFERENCE_ID = (uint32_t)-1;
    typedef std::map<std::string, uint32_t> ReferenceIdMap;
public:
    ReferenceIdManager()
        : _nextId(1) // 0 is reserved for delete flag
    {
    }
    ~ReferenceIdManager() {
    }
private:
    ReferenceIdManager(const ReferenceIdManager &);
    ReferenceIdManager& operator=(const ReferenceIdManager &);
public:
    uint32_t requireReferenceId(const std::string &refName) {
        auto it = _nameToId.find(refName);
        if (it != _nameToId.end()) {
            return it->second;
        }
        if (_nextId > MAX_REFERENCE_ID) {
            return INVALID_REFERENCE_ID;
        }
        uint32_t res = _nextId++;
        _nameToId[refName] = res;
        return res;
    }
public:
    // for test
    void reset() {
        _nameToId.clear();
        _nextId = 1;
    }
public:
    static bool isReferenceIdValid(uint32_t id) {
        return id != INVALID_REFERENCE_ID;
    }
private:
    uint32_t _nextId;
    ReferenceIdMap _nameToId;
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_HA3_REFERENCEIDMANAGER_H
