#ifndef __INDEXLIB_NUMERIC_LIMITS_H
#define __INDEXLIB_NUMERIC_LIMITS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <limits>
#include <autil/MultiValueType.h>

IE_NAMESPACE_BEGIN(index);

template<typename T>
struct NumericLimits {
    static T min() {return std::numeric_limits<T>::min();} 
    static T max() {return std::numeric_limits<T>::max();} 
};

template<>
struct NumericLimits<double> {
    static double min() {return -std::numeric_limits<double>::max();} 
    static double max() {return std::numeric_limits<double>::max();} 
};

template<>
struct NumericLimits<float> {
    static float min() {return -std::numeric_limits<float>::max();} 
    static float max() {return std::numeric_limits<float>::max();} 
};

template<>
struct NumericLimits<autil::MultiChar> {
    static autil::MultiChar min() {return autil::MultiChar();} 
    static autil::MultiChar max() {return autil::MultiChar();} 
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NUMERIC_LIMITS_H
