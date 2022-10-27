#ifndef ISEARCH_ACCUMULATOR_H
#define ISEARCH_ACCUMULATOR_H

#include <ha3/sql/common/common.h>

BEGIN_HA3_NAMESPACE(sql);

class Accumulator {
public:
    Accumulator() {}
    Accumulator(const Accumulator &) = delete;
    Accumulator &operator=(const Accumulator &) = delete;
    virtual ~Accumulator() {}
};

HA3_TYPEDEF_PTR(Accumulator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_ACCUMULATOR_H
