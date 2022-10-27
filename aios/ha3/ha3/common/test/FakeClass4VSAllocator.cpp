#include <ha3/common/test/FakeClass4VSAllocator.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, FakeClass4VSAllocator);

int FakeClass4VSAllocator::_constructCount = 0;
int FakeClass4VSAllocator::_destructCount = 0;

FakeClass4VSAllocator::FakeClass4VSAllocator() {
    _constructCount++;
}

FakeClass4VSAllocator::FakeClass4VSAllocator(const FakeClass4VSAllocator&) {
    _constructCount++;
}

FakeClass4VSAllocator::~FakeClass4VSAllocator() {
    _destructCount++;
}

int FakeClass4VSAllocator::getConstructCount() {
    return _constructCount;
}

int FakeClass4VSAllocator::getDestructCount() {
    return _destructCount;
}

void FakeClass4VSAllocator::resetCounts() {
    _constructCount = 0;
    _destructCount = 0;
}

END_HA3_NAMESPACE(common);
