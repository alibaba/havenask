%{
%}

%debug
%skeleton "lalr1.cc"
%name-prefix="isearch_sp"
%start query_node
%defines
%define "parser_class_name" "SPQueryParser"
%locations

%parse-param { isearch::sql::SPQueryScanner &scanner }
%parse-param { isearch::queryparser::ParserContext &ctx }
%code requires{
#include <stdio.h>
#include <string>
#include <vector>
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/Term.h"
#include "ha3/queryparser/QueryExpr.h"
#include "ha3/queryparser/AtomicQueryExpr.h"
#include "ha3/queryparser/WordsTermExpr.h"
#include "ha3/queryparser/OrTermQueryExpr.h"
#include "ha3/queryparser/AndTermQueryExpr.h"
#include <limits.h>
namespace isearch {
namespace queryparser {
class QueryParser;
class ParserContext;
} // namespace queryparser
} // namespace isearch

namespace isearch {
namespace sql {
class SPQueryScanner;
} // namespace sql
} // namespace isearch
}

%union{
    char* pData;
    isearch::queryparser::QueryExpr *pQuery;
    isearch::queryparser::AtomicQueryExpr *pAtomicQuery;
}

%token END 0

%token <pData> FIELD VALUE
%type <pQuery> syntax_node
%type <pAtomicQuery> term_node

%destructor { free($$); } FIELD
%destructor { free($$); } VALUE
%destructor { delete $$; } syntax_node term_node

%left '|'
%left '#'
%left '+'
%left '!'
%left ':'

%{

#include "ha3/queryparser/QueryParser.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/sql/ops/scan/builtin/SpQueryScanner.h"

#undef yylex
#define yylex scanner.lex

%}

%%

query_node: syntax_node {
    ctx.addQueryExpr($1);
    ctx.setStatus(isearch::queryparser::ParserContext::OK);
 }
;

syntax_node:
 term_node { $$ = $1; }
| FIELD ':' syntax_node {$$ = $3;
        std::string indexName($1);
        ctx.getParser().setCurrentIndex(indexName);
        $$->setLeafIndexName(indexName);
        free($1);
  }
| syntax_node '!' syntax_node { $$ = ctx.getParser().createAndNotExpr($1, $3); } //!a
| syntax_node '+' syntax_node { $$ = ctx.getParser().createAndExpr($1, $3); } //a+b
| syntax_node '+' { $$ = $1; } //a+
| '+' syntax_node { $$ = $2; } //+b
| syntax_node '|' syntax_node { $$ = ctx.getParser().createOrExpr($1, $3); } //a|b
| syntax_node '#' syntax_node { $$ = ctx.getParser().createOrExpr($1, $3); } //a#b
| '(' syntax_node ')' { $$ = $2; } //(a)
;

term_node : FIELD {
          std::string *text = new std::string($1); $$ = ctx.getParser().createWordsTermExpr(text);
          $$->setIndexName(ctx.getParser().getCurrentIndex());
          free($1);
          }

     | VALUE {
          std::string *text = new std::string($1); $$ = ctx.getParser().createWordsTermExpr(text);
          $$->setIndexName(ctx.getParser().getCurrentIndex());
          free($1);
          }

%%

void isearch_sp::SPQueryParser::error(const isearch_sp::SPQueryParser::location_type& l,
                                  const std::string& m)
{
}
