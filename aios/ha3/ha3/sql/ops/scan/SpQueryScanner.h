#ifndef ISEARCH_SPQUERY_SCANNER_H
#define ISEARCH_SPQUERY_SCANNER_H

#ifndef YY_DECL
#define YY_DECL                                                         \
    isearch_sp::SPQueryParser::token_type                                   \
    isearch::sql::SPQueryScanner::lex(                                        \
            isearch_sp::SPQueryParser::semantic_type* yylval,               \
            isearch_sp::SPQueryParser::location_type* yylloc)
#endif

#ifndef __FLEX_LEXER_H
#define yyFlexLexer SPQueryFlexLexer
#include "FlexLexer.h"
#undef yyFlexLexer
#endif

#include <ha3/sql/ops/scan/SpQueryParser.hh>

BEGIN_HA3_NAMESPACE(sql);

class SPQueryScanner : public SPQueryFlexLexer
{
public:
    SPQueryScanner(std::istream* yyin = 0, std::ostream* yyout = 0);
    ~SPQueryScanner();
public:
    virtual isearch_sp::SPQueryParser::token_type lex(
            isearch_sp::SPQueryParser::semantic_type* yylval,
            isearch_sp::SPQueryParser::location_type* yylloc);
private:
    HA3_LOG_DECLARE();  
};

END_HA3_NAMESPACE(sql);

#endif
