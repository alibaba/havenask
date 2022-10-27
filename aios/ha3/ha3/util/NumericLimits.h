#ifndef ISEARCH_NUMERICLIMITS_H
#define ISEARCH_NUMERICLIMITS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <limits>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(util);

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

END_HA3_NAMESPACE(util);

#endif //ISEARCH_NUMERICLIMITS_H
