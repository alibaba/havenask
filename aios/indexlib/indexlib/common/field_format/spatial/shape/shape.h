#ifndef __INDEXLIB_SHAPE_H
#define __INDEXLIB_SHAPE_H
#include <autil/StringUtil.h>
#include <autil/ConstString.h>
#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

class Rectangle;
DEFINE_SHARED_PTR(Rectangle);

class Point;
DEFINE_SHARED_PTR(Point);

class Shape;
DEFINE_SHARED_PTR(Shape);
struct DisjointEdges
{
    DisjointEdges()
        : edgesCache(0)
        {}
    void clear() { edgesCache = 0; }
    bool hasDisjointEdge() { return edgesCache != 0; }
    void setDisjointEdge(size_t idx)
        {
            if (idx < (size_t)64)
            {
                edgesCache |= ((uint64_t)1 << idx);
            }
        }
    bool isDisjointEdge(size_t idx)
        {
            if (idx < (size_t)64)
            {
                return edgesCache & ((uint64_t)1 << idx);
            }
            return false;
        }
    uint64_t edgesCache;
};

class Shape
{
public:
    enum Relation
    {
        CONTAINS, //contains other shape
        WITHINS,  //within other shape
        INTERSECTS,
        DISJOINTS
    };

    enum ShapeType
    {
        POINT,
        RECTANGLE,
        CIRCLE,
        POLYGON,
        LINE,
        UNKOWN
    };

public:
    Shape() {}
    virtual ~Shape() {}

public:
    virtual Relation GetRelation(const Rectangle* other,
                                 DisjointEdges& disjointEdges) const = 0;
    virtual ShapeType GetType() const = 0;
    virtual RectanglePtr GetBoundingBox() const = 0;
    virtual std::string ToString() const = 0;

public:
    bool IsInnerCoordinate(double lon, double lat) const
    {
        if (!Shape::IsValidCoordinate(lon, lat))
        {
            return false;
        }
        return CheckInnerCoordinate(lon, lat);
    }

protected:
    static const double MAX_X;
    static const double MAX_Y;
    static const double MIN_X;
    static const double MIN_Y;

    static bool StringToValueVec(const autil::ConstString& str,
                                 std::vector<double>& values,
                                 const std::string& delim);
    static bool StringToValueVec(const std::string& str,
                                 std::vector<double>& values,
                                 const std::string& delim);
    static RectanglePtr GetBoundingBox(const std::vector<Point>& mPoitVec);
    static bool IsValidCoordinate(double lon, double lat);

protected:
    // return true when point(lon, lat) in shape
    virtual bool CheckInnerCoordinate(double lon, double lat) const = 0;

protected:
    mutable RectanglePtr mBoundingBox;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPE_H
