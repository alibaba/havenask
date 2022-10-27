%{
#include <ha3/sql/ops/scan/SpQueryScanner.h>

typedef isearch_sp::SPQueryParser::token token;
typedef isearch_sp::SPQueryParser::token_type token_type;

#define yyterminate() return token::END
#define YY_NO_UNISTD_H
#define YY_USER_ACTION  yylloc->columns(yyleng);
%}

%option c++
%option batch
%option noyywrap
%option prefix="SPQuery"

%{

%}

LEX_ALPHA [A-Za-z_.]+
LEX_DIGIT [0-9]+

LEX_WORD_SIGN [\*%$\ -]
LEX_CJK_1 (\xe2[\xba-\xbf][\x80-\xbf])
LEX_CJK_2 ([\xe3-\xe9][\x80-\xbf][\x80-\xbf])

%%

[#&=+!|^~,:;\(\)\[\]\{\}] { return static_cast<token_type>(*yytext); }

({LEX_ALPHA}|{LEX_DIGIT})+ { yylval->pData = strdup(yytext); return token::FIELD; }
({LEX_ALPHA}|{LEX_DIGIT}|{LEX_WORD_SIGN}|{LEX_CJK_1}|{LEX_CJK_2})+ { yylval->pData = strdup(yytext); return token::VALUE; }

%%
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SPQueryScanner);

SPQueryScanner::SPQueryScanner(std::istream* in, std::ostream* out)
    : yyFlexLexer(in, out)
{
    //yy_flex_debug = true;
}

SPQueryScanner::~SPQueryScanner() {
}

END_HA3_NAMESPACE(sql);


#ifdef yylex
#undef yylex
#endif

int SPQueryFlexLexer::yylex() {
    return 0;
}
