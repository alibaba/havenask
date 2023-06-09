%{
#include "expression/common.h"
#include "expression/syntax/BisonParser.hh"
#include "expression/syntax/Scanner.h"

using namespace syntax_bison;

typedef BisonParser::token token;
typedef BisonParser::token_type token_type;

#define yyterminate() return token::END

#undef YY_DECL
#define	YY_DECL                                                \
    token_type                                                 \
        expression::Scanner::lex(                              \
                semantic_type* yylval,                         \
                location_type* yylloc                          \
                                                               )

#define YY_USER_ACTION yylloc->begin.column = yycolumn;\
    yylloc->end.column = yycolumn + yyleng;\
    yycolumn += yyleng;

%}

%option c++
%option noyywrap
%option prefix="expression_scanner"
%option yyClass="Scanner"

%option debug
%x single_quoted
%x double_quoted

DIGIT            [0-9]
HEXDIGIT         [0-9a-fA-F]
ID_FIRST_CHAR    [_[:alpha:]]
ID_CHAR          [_[:alnum:]]
SPECIAL_SYMBOL   [\%()+*\-/|^&@~:#,;\[\]\{\}]
%%

AND {
    AUTIL_LOG(TRACE2, "|%s|AND|",YYText());
    return token::AND;
}

OR   {
    AUTIL_LOG(TRACE2, "|%s|OR|",YYText());
    return token::OR;
}

0(x|X){HEXDIGIT}+ {
    AUTIL_LOG(TRACE2, "|%s|HEX INTEGER|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::INTEGER;
}

{DIGIT}+\.{DIGIT}+ { 
    AUTIL_LOG(TRACE2, "|%s|FLOAT|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::FLOAT;
}

{DIGIT}+ { 
    AUTIL_LOG(TRACE2, "|%s|INTEGER|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::INTEGER;
}

{ID_FIRST_CHAR}{ID_CHAR}* {
    AUTIL_LOG(TRACE2, "|%s|IDENTIFIER|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::IDENTIFIER;
}

"=" {
    AUTIL_LOG(TRACE2, "|%s|EQUAL|",YYText());
    return token::EQUAL;
}

">=" {
    AUTIL_LOG(TRACE2, "|%s|GE|",YYText());
    return token::GE;
}

"<=" {
    AUTIL_LOG(TRACE2, "|%s|LE|",YYText());
    return token::LE;
}

"<" {
    AUTIL_LOG(TRACE2, "|%s|LESS|",YYText());
    return token::LESS;
}

">" {
    AUTIL_LOG(TRACE2, "|%s|GREATER|",YYText());
    return token::GREATER;
}

"!=" {
    AUTIL_LOG(TRACE2, "|%s|NOT EQUAL|",YYText());
    return token::NE;
}

{SPECIAL_SYMBOL} {
/* [()+@*-/] */
    AUTIL_LOG(TRACE2, "|%s|",YYText());
    return static_cast<token_type>(*YYText());
}

<*>[ \t]+ {
    AUTIL_LOG(TRACE2, "|%s|WHITSPACE|",YYText());
}

\" {
    BEGIN(double_quoted);
    AUTIL_LOG(TRACE2, "BEGIN double_quoted condition");
    oss.str("");
}

<double_quoted>{
    [^\\\"]+ {
        //LOG(TRACE2, "|%s|double_quoted_NEST_STRING|",YYText());
        oss << YYText();
    }
    \\[\\\"] {
        //LOG(TRACE2, "|%s|double_quoted_NEST_STRING|",YYText());
        oss << (const char *)(YYText() + 1);
    }
    \" {
        BEGIN(INITIAL);
        yylval->stringVal = new std::string(oss.str());
        AUTIL_LOG(TRACE2, "|%s|double_quoted_STRING|", oss.str().c_str());
        AUTIL_LOG(TRACE2, "END double_quoted condition");
        return token::PHRASE_STRING;
    }
}

\' {
    BEGIN(single_quoted);
    AUTIL_LOG(TRACE2, "BEGIN single_quoted condition");
    oss.str("");
}

<single_quoted>{
    [^\\\']+ {
        //LOG(TRACE2, "|%s|single_quoted_NEST_STRING|",YYText());
        oss << YYText();
    }
    \\[\\\'] {
        //LOG(TRACE2, "|%s|single_quoted_NEST_STRING|",YYText());
        oss << (const char *)(YYText() + 1);
    }
    \' {
        BEGIN(INITIAL);
        yylval->stringVal = new std::string(oss.str());
        AUTIL_LOG(TRACE2, "|%s|single_quoted_STRING|", oss.str().c_str());
        AUTIL_LOG(TRACE2, "END single_quoted condition");
        return token::PHRASE_STRING;
    }
}

<<EOF>> {
    AUTIL_LOG(DEBUG, "meet end");
    return token::END;
}

%%

namespace expression {
AUTIL_LOG_SETUP(expression, Scanner);

Scanner::Scanner(std::istream *input, std::ostream *output) 
    : yyFlexLexer(input, output)
    , yycolumn(1)
{
}

Scanner::~Scanner() {
}

}
