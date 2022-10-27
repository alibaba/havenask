#ifndef ISEARCH_ATTRIBUTECLAUSE_H
#define ISEARCH_ATTRIBUTECLAUSE_H

#include <set>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class AttributeClause : public ClauseBase
{
public:
    AttributeClause();
    ~AttributeClause();
private:
    AttributeClause(const AttributeClause &);
    AttributeClause& operator=(const AttributeClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::set<std::string>& getAttributeNames() const {
        return _attributeNames;
    }

    std::set<std::string>& getAttributeNames() {
        return _attributeNames;
    }

    void addAttribute(const std::string &name) {
        _attributeNames.insert(name);
    }

    template<typename Iterator>
    void setAttributeNames(Iterator begin, Iterator end) {
        _attributeNames.clear();
        _attributeNames.insert(begin, end);
    }
private:
    std::set<std::string> _attributeNames;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AttributeClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ATTRIBUTECLAUSE_H
