#ifndef ISEARCH_QUERYPARSER_SYNTAXEXPR_H
#define ISEARCH_QUERYPARSER_SYNTAXEXPR_H

#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <suez/turing/expression/syntax/FuncSyntaxExpr.h>
#include <suez/turing/expression/syntax/RankSyntaxExpr.h>
#include <ha3/common.h>

BEGIN_HA3_NAMESPACE(queryparser);

typedef suez::turing::SyntaxExpr SyntaxExpr; 
typedef suez::turing::SyntaxExprType SyntaxExprType; 

typedef suez::turing::AtomicSyntaxExpr AtomicSyntaxExpr; 
typedef suez::turing::BinarySyntaxExpr BinarySyntaxExpr; 
typedef suez::turing::FuncSyntaxExpr FuncSyntaxExpr; 
typedef suez::turing::RankSyntaxExpr RankSyntaxExpr; 

END_HA3_NAMESPACE(queryparser);



#endif
