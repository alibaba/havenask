#ifndef ISEARCH_SCANNER_H
#define ISEARCH_SCANNER_H
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <sstream>

#ifndef __FLEX_LEXER_H
#define yyFlexLexer QueryFlexLexer
#include <FlexLexer.h>
#undef yyFlexLexer
#endif

#include <ha3/queryparser/BisonParser.hh>
BEGIN_HA3_NAMESPACE(queryparser);
class Scanner : public QueryFlexLexer
{
public:
    Scanner(std::istream *input, std::ostream *output);
    virtual ~Scanner();
public: 
    typedef isearch_bison::BisonParser::token token;
    typedef isearch_bison::BisonParser::token_type token_type;
    typedef isearch_bison::BisonParser::semantic_type semantic_type;
    typedef isearch_bison::BisonParser::location_type location_type;
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
