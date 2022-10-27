#ifndef __INDEXLIB_LINE_H
#define __INDEXLIB_LINE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);
class Line;
DEFINE_SHARED_PTR(Line);

class Line : public Shape
{
public:
    Line();
    ~Line();
    
public:
    Shape::Relation GetRelation(const Rectangle* other,
        DisjointEdges& disjointEdges) const override;
    Shape::ShapeType GetType() const override { return LINE; }
    RectanglePtr GetBoundingBox() const override
    {
        if (!mBoundingBox)
        {
            mBoundingBox = Shape::GetBoundingBox(mPointVec);
        }
        return mBoundingBox;
    }
    std::string ToString() const override;
    bool CheckInnerCoordinate(double lon, double lat) const override
    { assert(false); return false; }

public:
    static LinePtr FromString(const std::string& shapeStr);
    static LinePtr FromString(const autil::ConstString& shapeStr);

public:
    bool IsLegal() { return mPointVec.size() >= MIN_LINE_POINT_NUM; }
    const std::vector<Point>& GetPoints() const { return mPointVec; }

private:
    void AppendPoint(const Point& point) { mPointVec.push_back(point); }

private:
    static const size_t MIN_LINE_POINT_NUM;
    std::vector<Point> mPointVec;

private:
    friend class ShapeCovererTest;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LINE_H
