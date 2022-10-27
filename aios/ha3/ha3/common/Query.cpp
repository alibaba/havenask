#include <ha3/common/Query.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/common/TableQuery.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, Query);
#define QUERY_EXTEND_SERIALIZE_SECTION_NAME "HA3_QUERY_EXTEND"

Query::Query() : _queryRestrictor(false), _matchDataLevel(MDL_NONE) {

}

Query::Query(const Query &other) :
    _queryRestrictor(other._queryRestrictor),
    _matchDataLevel(other._matchDataLevel),
    _queryLabel(other._queryLabel)
{
    for (QueryVector::const_iterator it = other._children.begin();
         it != other._children.end(); ++it)
    {
        addQuery(QueryPtr((*it)->clone()));
    }
}

Query::~Query() {
}

std::ostream& operator << (std::ostream& out, const Query& query) {
    return out << query.toString();
}

std::string Query::toString() const {
    std::stringstream ss;
    ss << getQueryName();
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    ss << ":[" ;
    for (QueryVector::const_iterator it = _children.begin();
         it != _children.end(); it++)
    {
        if ( NULL != (*it) ) {
            ss << (*it)->toString() << ", ";
        } else {
            ss << "NULL" << ", ";
        }
    }
    ss << "]";
    return ss.str();
}

void Query::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_children);
    serializeMDLandQL(dataBuffer);
}

void Query::serializeMDLandQL(autil::DataBuffer &dataBuffer) const {
    autil::DataBuffer *extFieldsBuffer =
        dataBuffer.declareSectionBuffer(QUERY_EXTEND_SERIALIZE_SECTION_NAME);
    if (extFieldsBuffer) {
        extFieldsBuffer->write(_matchDataLevel);
        extFieldsBuffer->write(_queryLabel);
    }
}

void Query::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_children);
    deserializeMDLandQL(dataBuffer);
}

void Query::deserializeMDLandQL(autil::DataBuffer &dataBuffer) {
    autil::DataBuffer *extFieldsBuffer =
        dataBuffer.findSectionBuffer(QUERY_EXTEND_SERIALIZE_SECTION_NAME);
    if (extFieldsBuffer) {
        extFieldsBuffer->read(_matchDataLevel);
        extFieldsBuffer->read(_queryLabel);
    }
}

void Query::setQueryLabelBinary(const std::string &label) {
    auto level = label.empty() ? MDL_NONE : MDL_SUB_QUERY;
    setMatchDataLevel(level);
    setQueryLabel(label);
}

void Query::setQueryLabelTerm(const std::string& label) {
    setMatchDataLevel(MDL_TERM);
    setQueryLabel(label);
}

Query* Query::createQuery(QueryType type)
{
    Query *t = NULL;
    switch (type) {
    case TERM_QUERY:
        {
            RequiredFields requiredFields;
            t = new TermQuery("", "", requiredFields, "");
        }
        break;
    case NUMBER_QUERY:
        {
            RequiredFields requiredFields;
            t = new NumberQuery(0, "", requiredFields, "");
        }
        break;
    case PHRASE_QUERY:
        t = new PhraseQuery("");
        break;
    case AND_QUERY:
        t = new AndQuery("");
        break;
    case OR_QUERY:
        t = new OrQuery("");
        break;
    case RANK_QUERY:
        t = new RankQuery("");
        break;
    case ANDNOT_QUERY:
        t = new AndNotQuery("");
        break;
    case MULTI_TERM_QUERY:
        t = new MultiTermQuery("");
        break;
    case TABLE_QUERY:
        t = new TableQuery(string());
        break;
    default:
        assert(false);
    }

    return t;
}

END_HA3_NAMESPACE(common);
