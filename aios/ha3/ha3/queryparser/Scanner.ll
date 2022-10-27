%{
#include <ha3/common.h>
#include <string>
#include <ha3/queryparser/BisonParser.hh>

#include <ha3/queryparser/Scanner.h>

using namespace isearch_bison;
//BEGIN_NAMESPACE(queryparser);
typedef BisonParser::token token;
typedef BisonParser::token_type token_type;

#define yyterminate() return token::END


#undef YY_DECL
#define	YY_DECL						       \
    token_type                                                 \
    isearch::queryparser::Scanner::lex(                        \
            semantic_type* yylval,                             \
            location_type* yylloc                              \
    )
%}

%option c++
%option noyywrap
%option prefix="Query"
%option yyClass="Scanner"

%option debug
%x quoted double_quoted

DIGIT            [0-9] 
ID_FIRST_CHAR    [_[:alpha:]] 
ID_CHAR          [_[:alnum:]] 

SIGNED_CHAR  [+-]

CJK_1 (\xe2[\xba-\xbf][\x80-\xbf]) 
CJK_2 ([\xe3-\xe9][\x80-\xbf][\x80-\xbf])
FULLWIDTH_1 (\xef[\xbc-\xbe][\x80-\xbf])
FULLWIDTH_2 (\xef\xbf[\x80-\xaf])

SPECIAL_SYMBOL   [:;\]\[()^,|&#@]
USABLE_SYMBOL    [.\-+*/!_`?~\\%$]
%%

AND {
     HA3_LOG(TRACE2, "|%s|AND|",YYText());
     return token::AND;
 }

OR   {
    HA3_LOG(TRACE2, "|%s|OR|",YYText());
    return token::OR;
}

ANDNOT {
     HA3_LOG(TRACE2, "|%s|ANDNOT|",YYText());
     return token::ANDNOT;
 }

RANK   {
    HA3_LOG(TRACE2, "|%s|RANK|",YYText());
    return token::RANK;
}

{SIGNED_CHAR}?{DIGIT}+ { 
    HA3_LOG(TRACE2, "|%s|NUMBER|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
     return token::NUMBER;
 }

{ID_FIRST_CHAR}{ID_CHAR}* {
    HA3_LOG(TRACE2, "|%s|IDENTIFIER|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::IDENTIFIER;
}

({CJK_1}|{CJK_2}|{FULLWIDTH_1}|{FULLWIDTH_2}|{ID_CHAR}|{USABLE_SYMBOL})+ {
    HA3_LOG(TRACE2, "|%s|CJK_STRING|",YYText());
    yylval->stringVal = new std::string(YYText(), YYLeng());
    return token::CJK_STRING;
}

\" {
    BEGIN(double_quoted);
    HA3_LOG(TRACE2, "BEGIN double_quoted condition");
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
        HA3_LOG(TRACE2, "|%s|double_quoted_STRING|", oss.str().c_str());
        HA3_LOG(TRACE2, "END double_quoted condition");
        return token::PHRASE_STRING;
    }
}

\' {
    BEGIN(quoted);
    HA3_LOG(TRACE2, "BEGIN quoted condition");
    oss.str("");
}

<quoted>{
    [^\\\']+ {
        //LOG(TRACE2, "|%s|quoted_NEST_STRING|",YYText());
        oss << YYText();
    }
    \\[\\\'] {
        //LOG(TRACE2, "|%s|quoted_NEST_STRING|",YYText());
        oss << (YYText() + 1);
    }
    \' {
        yylval->stringVal = new std::string(oss.str());
        HA3_LOG(TRACE2, "|%s|single_quoted_STRING|", oss.str().c_str());
        BEGIN(INITIAL);
        HA3_LOG(TRACE2, "END single_quoted condition");
        return token::CJK_STRING;
    }
}

"=" {
    HA3_LOG(TRACE2, "|%s|EQUAL|",YYText());
    return token::EQUAL;
}

">=" {
    HA3_LOG(TRACE2, "|%s|GE|",YYText());
    return token::GE;
}

"<=" {
    HA3_LOG(TRACE2, "|%s|LE|",YYText());
    return token::LE;
}

"<" {
    HA3_LOG(TRACE2, "|%s|LESS|",YYText());
    return token::LESS;
}

">" {
    HA3_LOG(TRACE2, "|%s|GREATER|",YYText());
    return token::GREATER;
}

"||" {
    HA3_LOG(TRACE2, "|%s|WEAKAND|",YYText());
    return token::WEAKAND;
}

")"[ \t]*":" {
    HA3_LOG(TRACE2, "|%s|RPARENT_COLON|",YYText());
    return token::RPARENT_COLON;
}

{SPECIAL_SYMBOL} {
/* [().:]  { */
    HA3_LOG(TRACE2, "|%s|",YYText());
    return static_cast<token_type>(*YYText());
}

<*>[ \t]+ {
    HA3_LOG(TRACE2, "|%s|WHITSPACE|",YYText());
}

<quoted,double_quoted><<EOF>> {
    HA3_LOG(DEBUG, "uncompleted quote or double_quote");
    return token::END;
}

%%
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, Scanner);

Scanner::Scanner(std::istream *input, std::ostream *output) 
    : yyFlexLexer(input, output)
{
}

Scanner::~Scanner() {
}

void Scanner::setDebug(bool debug) {
    yy_flex_debug = debug;
}

END_HA3_NAMESPACE(queryparser);
