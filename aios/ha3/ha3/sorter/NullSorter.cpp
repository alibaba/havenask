#include <ha3/sorter/NullSorter.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, NullSorter);

NullSorter::NullSorter()
    : Sorter("NULL")
{
}

NullSorter::~NullSorter() {
}

Sorter *NullSorter::clone() {
    return new NullSorter(*this);
}

bool NullSorter::beginSort(suez::turing::SorterProvider *provider) {
    return true;
}

void NullSorter::sort(suez::turing::SortParam &sortParam) {
}

void NullSorter::endSort() {
}

END_HA3_NAMESPACE(sorter);
