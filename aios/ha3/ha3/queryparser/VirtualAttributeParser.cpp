#include <ha3/queryparser/VirtualAttributeParser.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, VirtualAttributeParser);

VirtualAttributeParser::VirtualAttributeParser(ErrorResult *errorResult) 
    : _errorResult(errorResult)
{ 
}

VirtualAttributeParser::~VirtualAttributeParser() { 
}

common::VirtualAttributeClause* VirtualAttributeParser::createVirtualAttributeClause() {
    return new VirtualAttributeClause;
}

void VirtualAttributeParser::addVirtualAttribute(
        common::VirtualAttributeClause *virtualAttributeClause, 
        const std::string &name, 
        suez::turing::SyntaxExpr *syntaxExpr)
{
    assert(virtualAttributeClause);
    if (!virtualAttributeClause->addVirtualAttribute(name, syntaxExpr)) {
        delete syntaxExpr;
        _errorResult->resetError(ERROR_VIRTUALATTRIBUTE_CLAUSE,
                string("add virtualAttribute failed[") + name +
                string("], same name virtualAttribute may has already existed"));
    }
}

END_HA3_NAMESPACE(queryparser);

