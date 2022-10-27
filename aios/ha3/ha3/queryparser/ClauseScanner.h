#ifndef ISEARCH_CLAUSESCANNER_H
#define ISEARCH_CLAUSESCANNER_H
#include <ha3/util/Log.h>
#include <sstream>

#ifndef __FLEX_LEXER_H
#define yyFlexLexer ClauseFlexLexer
#include <FlexLexer.h>
#undef yyFlexLexer
#endif

#include <ha3/queryparser/ClauseBisonParser.hh>
BEGIN_HA3_NAMESPACE(queryparser);
class ClauseScanner : public ClauseFlexLexer
{
public:
    ClauseScanner(std::istream *input, std::ostream *output);
    virtual ~ClauseScanner();
public:
    typedef isearch_bison::ClauseBisonParser::token token;
    typedef isearch_bison::ClauseBisonParser::token_type token_type;
    typedef isearch_bison::ClauseBisonParser::semantic_type semantic_type;
    typedef isearch_bison::ClauseBisonParser::location_type location_type;
public:
    virtual token_type lex(semantic_type* yylval, location_type* yylloc);
public:
    void setDebug(bool debug);
private:
    std::ostringstream oss;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);
#endif 
