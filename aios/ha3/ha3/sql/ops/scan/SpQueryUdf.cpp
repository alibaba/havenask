#include <ha3/sql/ops/scan/SpQueryUdf.h>
#include <ha3/sql/ops/scan/SpQueryScanner.h>
#include <ha3/sql/ops/scan/SpQueryParser.hh>
#include <sstream>
#include <autil/StringUtil.h>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(search, SPQueryUdf);

SPQueryUdf::SPQueryUdf(const char *defaultIndex)
    : _queryParser(defaultIndex, OP_AND, false)
{
}

SPQueryUdf::~SPQueryUdf() {
}

ParserContext* SPQueryUdf::parse(const char *queryText) {
    istringstream iss(queryText);
    ostringstream oss;
    SPQueryScanner scanner(&iss, &oss);
    ParserContext *ctx = new ParserContext(_queryParser);
    HA3_LOG(TRACE3, "context %p", ctx);
    isearch_sp::SPQueryParser parser(scanner, *ctx);
    if (parser.parse() != 0) {
        HA3_LOG(DEBUG, "%s:|%s|", ctx->getStatusMsg().c_str(), queryText);
        if (oss.str().length() > 0) {
            HA3_LOG(WARN, "failed to resolve  %s", oss.str().c_str());
        }
    }
    _queryParser.evaluateQuery(ctx);
    return ctx;
}

END_HA3_NAMESPACE(sql);