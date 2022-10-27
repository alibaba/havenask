#ifndef ISEARCH_VIRTUALATTRIBUTEPARSER_H
#define ISEARCH_VIRTUALATTRIBUTEPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <ha3/common/ErrorResult.h>

BEGIN_HA3_NAMESPACE(queryparser);

class VirtualAttributeParser
{
public:
    VirtualAttributeParser(common::ErrorResult *errorResult);
    ~VirtualAttributeParser();
private:
    VirtualAttributeParser(const VirtualAttributeParser &);
    VirtualAttributeParser& operator=(const VirtualAttributeParser &);
public:
    common::VirtualAttributeClause* createVirtualAttributeClause();
    void addVirtualAttribute(common::VirtualAttributeClause *virtualAttributeClause, 
                             const std::string &name, 
                             suez::turing::SyntaxExpr *syntaxExpr);
private:
    common::ErrorResult *_errorResult;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(VirtualAttributeParser);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_VIRTUALATTRIBUTEPARSER_H
