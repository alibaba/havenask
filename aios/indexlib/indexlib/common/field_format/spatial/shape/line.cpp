#include "indexlib/common/field_format/spatial/shape/line.h"
#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/shape/rectangle_intersect_operator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, Line);

const size_t Line::MIN_LINE_POINT_NUM = 2;

Line::Line()
{
}

Line::~Line() 
{
}

Shape::Relation Line::GetRelation(const Rectangle* other,
                                  DisjointEdges& disjointEdges) const
{
    auto relation = RectangleIntersectOperator::GetRelation(
            *other, *this, disjointEdges);
    if (relation == Shape::CONTAINS)
    {
        return WITHINS;
    }
    return relation;
}

LinePtr Line::FromString(const string& shapeStr)
{
    return FromString(autil::ConstString(shapeStr));
}

LinePtr Line::FromString(const autil::ConstString& shapeStr)
{
    LinePtr line(new Line);

    vector<ConstString> poiStrVec = 
        autil::StringTokenizer::constTokenize(shapeStr, ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < poiStrVec.size(); i++)
    {
        PointPtr point = Point::FromString(poiStrVec[i]);
        if (!point)
        {
            IE_LOG(WARN, "line point [%s] not valid", poiStrVec[i].c_str());
            return LinePtr();
        }
        line->AppendPoint(*point);
    }

    if (line->IsLegal())
    {
        return line;
    }
    
    return LinePtr();
}

string Line::ToString() const
{
    stringstream ss;
    ss << "line[";
    for (size_t i = 0; i < mPointVec.size() - 1; i++)
    {
        ss << mPointVec[i].ToString() << ",";
    }
    ss << mPointVec[mPointVec.size() - 1].ToString() << "]";
    return ss.str();
}

IE_NAMESPACE_END(common);

