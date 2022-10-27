%{
%}

%debug
%skeleton "lalr1.cc"
%name-prefix="isearch_sp"
%start query_node
%defines
%define "parser_class_name" "SPQueryParser"
%locations
%parse-param { HA3_NS(sql)::SPQueryScanner &scanner }
%parse-param { HA3_NS(queryparser)::ParserContext &ctx }
%code requires{
#include <stdio.h>
#include <string>
#include <vector>
#include <ha3/common/Query.h>
#include <ha3/common.h>
#include <ha3/common/Term.h>
#include <ha3/isearch.h>
#include <ha3/common/Term.h>
#include <ha3/queryparser/QueryExpr.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/WordsTermExpr.h>
#include <ha3/queryparser/OrTermQueryExpr.h>
#include <ha3/queryparser/AndTermQueryExpr.h>
#include <limits.h>
BEGIN_HA3_NAMESPACE(queryparser);
class QueryParser;
class ParserContext;
END_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(sql);
class SPQueryScanner;
END_HA3_NAMESPACE(sql);
}

%union{
    char* pData;
    HA3_NS(queryparser)::QueryExpr *pQuery;
    HA3_NS(queryparser)::AtomicQueryExpr *pAtomicQuery;
}

%token END 0

%token <pData> FIELD VALUE
%type <pQuery> syntax_node
%type <pAtomicQuery> term_node

%destructor { free($$); } FIELD
%destructor { free($$); } VALUE
%destructor { delete $$; } syntax_node term_node

%left '|'
%left '+'
%left '!'
%left ':'

%{

#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/ParserContext.h>
#include <ha3/sql/ops/scan/SpQueryScanner.h>

#undef yylex
#define yylex scanner.lex

%}

%%

query_node: syntax_node {
    ctx.addQueryExpr($1);
    ctx.setStatus(HA3_NS(queryparser)::ParserContext::OK);
 }
;

syntax_node:
 term_node { $$ = $1; }
| FIELD ':' syntax_node {$$ = $3;
        std::string indexName($1);
        ctx.getParser().setCurrentIndex(indexName);
        $$->setLeafIndexName(indexName);
  }
| syntax_node '!' syntax_node { $$ = ctx.getParser().createAndNotExpr($1, $3); } //!a
| syntax_node '+' syntax_node { $$ = ctx.getParser().createAndExpr($1, $3); } //a+b
| syntax_node '+' { $$ = $1; } //a+
| '+' syntax_node { $$ = $2; } //+b
| syntax_node '|' syntax_node { $$ = ctx.getParser().createOrExpr($1, $3); } //a|b
| '(' syntax_node ')' { $$ = $2; } //(a)
;

term_node : FIELD {
          std::string *text = new std::string($1); $$ = ctx.getParser().createWordsTermExpr(text);
          $$->setIndexName(ctx.getParser().getCurrentIndex());
          }

     | VALUE {
          std::string *text = new std::string($1); $$ = ctx.getParser().createWordsTermExpr(text);
          $$->setIndexName(ctx.getParser().getCurrentIndex());
          }

%%

void isearch_sp::SPQueryParser::error(const isearch_sp::SPQueryParser::location_type& l,
                                  const std::string& m)
{
}
