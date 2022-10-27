#ifndef ISEARCH_FAKECLASS4VSALLOCATOR_H_
#define ISEARCH_FAKECLASS4VSALLOCATOR_H_

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/Trait.h>

BEGIN_HA3_NAMESPACE(common);

class FakeClass4VSAllocator
{
public:
    FakeClass4VSAllocator();
    FakeClass4VSAllocator(const FakeClass4VSAllocator &other);
    ~FakeClass4VSAllocator();
    static int getConstructCount();

    static int getDestructCount();
    static void resetCounts();
private:
    static int _constructCount;
    static int _destructCount;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

NOT_SUPPORT_SERIZLIE_TYPE(isearch::common::FakeClass4VSAllocator);

#endif
