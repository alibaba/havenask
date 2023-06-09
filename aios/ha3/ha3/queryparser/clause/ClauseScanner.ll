%{
#include <string>
#include "ha3/queryparser/ClauseBisonParser/ClauseBisonParser.hh"
#include "ha3/queryparser/ClauseScanner.h"

using namespace isearch_bison;
//BEGIN_NAMESPACE(queryparser);
typedef ClauseBisonParser::token token;
typedef ClauseBisonParser::token_type token_type;

#define yyterminate() return token::END

#undef YY_DECL
#define	YY_DECL						       \
    token_type                                                 \
        isearch::queryparser::ClauseScanner::lex(              \
            semantic_type* yylval,                             \
            location_type* yylloc                              \
    )
%}

%option c++
%option noyywrap
%option prefix="Clause"
%option yyClass="ClauseScanner"

%option debug
%x double_quoted

DIGIT            [0-9]
HEXDIGIT         [0-9a-fA-F]
ID_FIRST_CHAR    [_[:alpha:]]
ID_CHAR          [_[:alnum:]]
SPECIAL_SYMBOL   [\%()+*\-/|^&~:#,;\[\]\{\}]
%%

AND {
    AUTIL_LOG(TRACE2, "|%s|AND|",YYText());
    return token::AND;
}

OR   {
    AUTIL_LOG(TRACE2, "|%s|OR|",YYText());
    return token::OR;
}

"dist_key" {
   return token::DISTINCT_KEY;
}

"dist_count" {
   return token::DISTINCT_COUNT;
}

"dist_times" {
   return token::DISTINCT_TIMES;
}

"max_item_count" {
   return token::DISTINCT_MAX_ITEM_COUNT;
}

"reserved" {
   return token::DISTINCT_RESERVED;
}

"update_total_hit" {
    return token::DISTINCT_UPDATE_TOTAL_HIT;
}

"dbmodule" {
   return token::MODULE_NAME;
}

"dist_filter" {
   return token::DISTINCT_FILTER_CLAUSE;
}

"grade" {
   return token::GRADE_THRESHOLDS;
}

"group_key" {
   AUTIL_LOG(TRACE2, "|%s|GROUP KEY|",YYText());
   return token::GROUP_KEY;
}

"range" {
   AUTIL_LOG(TRACE2, "|%s|RANGE KEY|",YYText());
   return token::RANGE_KEY;
}

"agg_fun" {
   AUTIL_LOG(TRACE2, "|%s|FUNCTION KEY|",YYText());
   return token::FUNCTION_KEY;
}

"max" {
   AUTIL_LOG(TRACE2, "|%s|MAX FUN|",YYText());
   return token::MAX_FUN;
}

"min" {
   AUTIL_LOG(TRACE2, "|%s|MIN FUN|",YYText());
   return token::MIN_FUN;
}

"count" {
   AUTIL_LOG(TRACE2, "|%s|COUNT FUN|",YYText());
   return token::COUNT;
}

"sum" {
   AUTIL_LOG(TRACE2, "|%s|SUM FUN|",YYText());
   return token::SUM_FUN;
}

"distinct_count" {
   AUTIL_LOG(TRACE2, "|%s|DISTINCT_COUNT FUN|",YYText());
   return token::DISTINCT_COUNT_FUN;
}

"agg_filter" {
   AUTIL_LOG(TRACE2, "|%s|AGGREGATE FILTER|",YYText());
   return token::AGGREGATE_FILTER;
}
"max_group" {
   AUTIL_LOG(TRACE2, "|%s|MAX_GROUP|",YYText());
   return token::MAX_GROUP;
}
"order_by" {
   AUTIL_LOG(TRACE2, "|%s|ORDER_BY|",YYText());
   return token::ORDER_BY;
}
"agg_sampler_threshold" {
   AUTIL_LOG(TRACE2, "|%s|AGG_SAMPLER_THRESHOLD|",YYText());
   return token::AGG_SAMPLER_THRESHOLD;
}

"agg_sampler_step" {
   AUTIL_LOG(TRACE2, "|%s|AGG_SAMPLER_STEP|",YYText());
   return token::AGG_SAMPLER_STEP;
}

"quota" {
   AUTIL_LOG(TRACE2, "|%s|quota|",YYText());
   return token::QUOTA_KEY;
}

"UNLIMITED" {
   AUTIL_LOG(TRACE2, "|%s|quota|",YYText());
   return token::UNLIMITED_KEY;
}

"key" {
    AUTIL_LOG(TRACE2, "|%s|key|", YYText());
    return token::CACHE_KEY;
}

"use" {
    AUTIL_LOG(TRACE2, "|%s|use|", YYText());
    return token::CACHE_USE;
}

"cur_time" {
    AUTIL_LOG(TRACE2, "|%s|cur_time|", YYText());
    return token::CACHE_CUR_TIME;
}

"expire_time" {
    AUTIL_LOG(TRACE2, "|%s|expire_time|", YYText());
    return token::CACHE_EXPIRE_TIME;
}

"cache_filter" {
    AUTIL_LOG(TRACE2, "|%s|cache_filter|", YYText());
    return token::CACHE_FILTER;
}

"cache_doc_num_limit" {
    AUTIL_LOG(TRACE2, "|%s|cache_doc_num_limit|", YYText());
    return token::CACHE_DOC_NUM_LIMIT;
}

"refresh_attributes" {
    AUTIL_LOG(TRACE2, "|%s|refresh_attributes|", YYText());
    return token::REFRESH_ATTRIBUTES;
}

"sort" {
    AUTIL_LOG(TRACE2, "|%s|sort_keys|", YYText());
    return token::SORT_KEY;
}

"percent" {
    AUTIL_LOG(TRACE2, "|%s|percent|", YYText());
    return token::SORT_PERCENT;
}

"RANK" {
    AUTIL_LOG(TRACE2, "|%s|RANK|", YYText());
    return token::RANK_EXPRESSION;
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
/* [()+*-/] */
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

<<EOF>> {
    AUTIL_LOG(DEBUG, "meet end");
    return token::END;
}

%%

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, ClauseScanner);

ClauseScanner::ClauseScanner(std::istream *input, std::ostream *output)
    : yyFlexLexer(input, output)
{
}

ClauseScanner::~ClauseScanner() {
}

void ClauseScanner::setDebug(bool debug) {
    yy_flex_debug = debug;
}

} // namespace queryparser
} // namespace isearch
