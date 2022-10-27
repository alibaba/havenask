#include <ha3/common/VirtualAttributeClause.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, VirtualAttributeClause);

VirtualAttributeClause::VirtualAttributeClause() { 
}

VirtualAttributeClause::~VirtualAttributeClause() { 
    for (vector<VirtualAttribute*>::iterator it = _virtualAttributes.begin(); 
         it != _virtualAttributes.end(); ++it) 
    {
        delete (*it);
    }
    _virtualAttributes.clear();
}

void VirtualAttributeClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_virtualAttributes);
}

void VirtualAttributeClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_virtualAttributes);
}

bool VirtualAttributeClause::addVirtualAttribute(const std::string &name, 
                           SyntaxExpr *syntaxExpr) 
{
    if (name.empty() || NULL == syntaxExpr) {
        HA3_LOG(WARN, "add virtualAttribute failed, name[%s] is empty or"
                " syntaxExpr is NULL", name.c_str());
        return false;
    }
    for (vector<VirtualAttribute*>::const_iterator it = _virtualAttributes.begin(); 
         it != _virtualAttributes.end(); ++it) 
    {
        if ((*it)->getVirtualAttributeName() == name) {
            HA3_LOG(WARN, "add virtualAttribute failed, same name[%s] virtualAttribute "
                    "already existed", name.c_str());
            return false;
        }
    }
    
    _virtualAttributes.push_back(new VirtualAttribute(name, syntaxExpr));
    return true;
}

string VirtualAttributeClause::toString() const {
    string vaClauseStr;
    size_t vaCount = _virtualAttributes.size();
    for (size_t i = 0; i < vaCount; i++) {
        VirtualAttribute* virtualAttribute = _virtualAttributes[i];
        vaClauseStr.append("[");
        vaClauseStr.append(virtualAttribute->getVirtualAttributeName());
        vaClauseStr.append(":");
        vaClauseStr.append(virtualAttribute->getExprString());
        vaClauseStr.append("]");
    }
    return vaClauseStr;
}

END_HA3_NAMESPACE(common);

