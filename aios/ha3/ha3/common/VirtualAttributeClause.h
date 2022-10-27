#ifndef ISEARCH_VIRTUALATTRIBUTECLAUSE_H
#define ISEARCH_VIRTUALATTRIBUTECLAUSE_H

#include <string>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/util/VirtualAttribute.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class VirtualAttributeClause : public ClauseBase
{
public:
    VirtualAttributeClause();
    ~VirtualAttributeClause();
private:
    VirtualAttributeClause(const VirtualAttributeClause &);
    VirtualAttributeClause& operator=(const VirtualAttributeClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    bool addVirtualAttribute(const std::string &name, 
                             suez::turing::SyntaxExpr *syntaxExpr);
    size_t getVirtualAttributeSize() const {return _virtualAttributes.size();}
    const std::vector<suez::turing::VirtualAttribute*>& getVirtualAttributes() const {
        return _virtualAttributes;}
    suez::turing::VirtualAttribute* getVirtualAttribute(const std::string &name) const {
        std::vector<suez::turing::VirtualAttribute*>::const_iterator it = 
            _virtualAttributes.begin();
        for (; it != _virtualAttributes.end(); ++it) {
            if (name == (*it)->getVirtualAttributeName()) {
                return (*it);
            }
        }
        return NULL;
    }
private:
    std::vector<suez::turing::VirtualAttribute*> _virtualAttributes;
private:
    friend class VirtualAttributeClauseTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(VirtualAttributeClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_VIRTUALATTRIBUTECLAUSE_H
