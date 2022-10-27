#include "indexlib/common/field_format/spatial/shape/shape_coverer.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShapeCoverer);

ShapeCoverer::ShapeCoverer(int8_t indexTopLevel, int8_t indexDetailLevel,
                           bool onlyGetLeafCell)
    : mIndexTopLevel(indexTopLevel)
    , mIndexDetailLevel(indexDetailLevel)
    , mOnlyGetLeafCell(onlyGetLeafCell)
    , mCellCount(0)
{
    assert(GeoHashUtil::IsLevelValid(indexTopLevel));
    assert(GeoHashUtil::IsLevelValid(indexDetailLevel));
    assert(indexDetailLevel >= indexTopLevel);
}

ShapeCoverer::~ShapeCoverer() 
{
}

IE_NAMESPACE_END(common);

