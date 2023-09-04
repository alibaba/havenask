%{
%}

%skeleton "lalr1.cc"
%name-prefix="suez_turing_bison"
%debug
%start expression_final
%defines
%define "parser_class_name" "BisonParser"

%locations

%parse-param {suez::turing::Scanner &scanner}
%parse-param {suez::turing::SyntaxParser::ParseState &state}

%error-verbose
%debug

%code requires {

#include "suez/turing/expression/syntax/SyntaxExprFactory.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"

namespace suez {
namespace turing {
class Scanner;
}
}

}

%union {
    std::string *stringVal;
    suez::turing::SyntaxExpr *syntaxExprVal;
    suez::turing::FuncSyntaxExpr *funcExprVal;
    std::vector<suez::turing::SyntaxExpr*> *paramVal;
}

%token			END	     0	"end of file"
%token <stringVal> 	INTEGER  	"integer"
%token <stringVal> 	FLOAT		"float"
%token <stringVal> 	IDENTIFIER      "identifier"
%token <stringVal> 	PHRASE_STRING      "string NOT including symbols"

%token AND OR EQUAL LE GE LESS GREATER NE RANK_EXPRESSION LOG QUESTION COLON 

%type <syntaxExprVal> expression single_expression attribute_name rank_expression
%type <funcExprVal> function_expr
%type <paramVal> func_param

%destructor {delete $$;} IDENTIFIER PHRASE_STRING INTEGER FLOAT
%destructor {delete $$;} expression single_expression attribute_name
%destructor {delete $$;} function_expr
%destructor {
    for (auto it : *$$) {
        delete it;
    }
    delete $$;
} func_param

%{

#include "suez/turing/expression/syntax/Scanner.h"

#undef yylex
#define yylex scanner.lex
using namespace suez::turing;
%}

%left OR
%left AND
%left '|'
%left '^'
%left '&' '@'
%left EQUAL NE
%left LESS GREATER LE GE
%left '+' '-'
%left '*' '/'
%left LOG
%%

expression_final : expression END {
    state.setSyntax($1);
}

expression : single_expression  { $$ = $1; }
| '(' expression ')' { $$ = $2; }
| expression '+' expression { $$ = SyntaxExprFactory::createAddExpr($1, $3); }
| expression '-' expression { $$ = SyntaxExprFactory::createMinusExpr($1, $3); }
| expression '*' expression { $$ = SyntaxExprFactory::createMulExpr($1, $3); }
| expression '/' expression { $$ = SyntaxExprFactory::createDivExpr($1, $3); }
| expression '*' '*' expression { $$ = SyntaxExprFactory::createPowerExpr($1, $4); }
| LOG '(' expression ',' expression ')' {
    $$ = SyntaxExprFactory::createLogExpr($3, $5);
}
| expression AND expression {
    $$ = SyntaxExprFactory::createAndExpr($1, $3);
    if ($$ == NULL) {
        error(yylloc, "");
        YYERROR;
    }
}
| expression OR expression {
    $$ = SyntaxExprFactory::createOrExpr($1, $3);
    if ($$ == NULL) {
        error(yylloc, "");
        YYERROR;
    }
}
| expression EQUAL expression { $$ = SyntaxExprFactory::createEqualExpr($1, $3); }
| expression LESS expression { $$ = SyntaxExprFactory::createLessExpr($1, $3); }
| expression GREATER expression { $$ = SyntaxExprFactory::createGreaterExpr($1, $3); }
| expression NE expression { $$ = SyntaxExprFactory::createNotEqualExpr($1, $3); }
| expression LE expression { $$ = SyntaxExprFactory::createLessEqualExpr($1, $3); }
| expression GE expression { $$ = SyntaxExprFactory::createGreaterEqualExpr($1, $3); }
| expression '|' expression { $$ = SyntaxExprFactory::createBitOrExpr($1, $3); }
| expression '&' expression { $$ = SyntaxExprFactory::createBitAndExpr($1, $3); }
| expression '^' expression { $$ = SyntaxExprFactory::createBitXorExpr($1, $3); }
| expression QUESTION expression COLON expression { $$ = SyntaxExprFactory::createConditionalExpr($1, $3, $5); }

single_expression : INTEGER { $$ = SyntaxExprFactory::createAtomicExpr($1, INT64_VT); }
| '-' INTEGER { $$ = SyntaxExprFactory::createAtomicExpr($2, INT64_VT, false); }
| FLOAT { $$ = SyntaxExprFactory::createAtomicExpr($1, DOUBLE_VT); }
| '-' FLOAT { $$ = SyntaxExprFactory::createAtomicExpr($2, DOUBLE_VT, false); }
| INTEGER IDENTIFIER {
    if ("c" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, INT8_VT);
        delete $2;
    } else if ("uc" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, UINT8_VT);
        delete $2;
    } else if ("s" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, INT16_VT);
        delete $2;
    } else if ("us" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, UINT16_VT);
        delete $2;
    } else if ("i" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, INT32_VT);
        delete $2;
    } else if ("ui" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, UINT32_VT);
        delete $2;
    } else if ("l" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, INT64_VT);
        delete $2;
    } else if ("ul" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, UINT64_VT);
        delete $2;
    } else {
        delete $1;
        delete $2;
        error(yylloc, "");
        YYERROR;
    }
}
| '-' INTEGER IDENTIFIER {
    if ("c" == *$3) {
        $$ = SyntaxExprFactory::createAtomicExpr($2, INT8_VT, false);
        delete $3;
    } else if ("s" == *$3) {
        $$ = SyntaxExprFactory::createAtomicExpr($2, INT16_VT, false);
        delete $3;
    } else if ("i" == *$3) {
        $$ = SyntaxExprFactory::createAtomicExpr($2, INT32_VT, false);
        delete $3;
    } else if ("l" == *$3) {
        $$ = SyntaxExprFactory::createAtomicExpr($2, INT64_VT, false);
        delete $3;
    } else {
        delete $2;
        delete $3;
        error(yylloc, "");
        YYERROR;
    }
}
| FLOAT IDENTIFIER {
    if ("f" == *$2) {
        $$ = SyntaxExprFactory::createAtomicExpr($1, FLOAT_VT);
        delete $2;
    } else {
        delete $1;
        delete $2;
        error(yylloc, "");
        YYERROR;
    }
}
| '-' FLOAT IDENTIFIER {
    if ("f" == *$3) {
        $$ = SyntaxExprFactory::createAtomicExpr($2, FLOAT_VT, false);
        delete $3;
    } else {
        delete $2;
        delete $3;
        error(yylloc, "");
        YYERROR;
    }
}
| PHRASE_STRING { $$ = SyntaxExprFactory::createAtomicExpr($1, STRING_VT); }
| attribute_name { $$ = $1; }
| function_expr { $$ = $1; }
| rank_expression { $$ = $1; }

rank_expression : RANK_EXPRESSION { $$ = SyntaxExprFactory::createRankExpr(); }

function_expr : IDENTIFIER '(' ')' { $$ = SyntaxExprFactory::createFuncExpr($1); }
| IDENTIFIER '(' func_param ')' { $$ = SyntaxExprFactory::createFuncExpr($1, $3); }

attribute_name : IDENTIFIER {
    $$ = SyntaxExprFactory::createAtomicExpr($1, ATTRIBUTE_NAME);
}
| IDENTIFIER '.' IDENTIFIER {
    $$ = SyntaxExprFactory::createAtomicExpr($3, ATTRIBUTE_NAME, true, $1);
}

func_param : expression { $$ = new std::vector<SyntaxExpr*>; $$->push_back($1); }
     | func_param ',' expression { $1->push_back($3); $$ = $1; }

%%
void suez_turing_bison::BisonParser::error(const suez_turing_bison::BisonParser::location_type& l,
                                      const std::string& m)
{
    std::stringstream ss;
    std::string context = scanner.orgContext();
    ss << "'" << std::string(context.data(), context.data() + l.begin.column) << "' " << m;
    state.fail(ss.str());
}
