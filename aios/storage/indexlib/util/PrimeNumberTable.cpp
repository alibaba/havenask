/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/util/PrimeNumberTable.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iosfwd>

using namespace std;

namespace indexlib { namespace util {

static int64_t PRIME_NUMBER_TABLE[] = {
    7,          1999,       2399,       2879,       3449,       4139,       4967,       5953,       7129,
    8543,       10253,      12301,      14759,      17707,      21247,      25471,      30559,      36671,
    43997,      52783,      63337,      76003,      91199,      109433,     131321,     157579,     189067,
    226871,     272231,     326663,     391987,     470359,     564419,     677239,     812681,     975217,
    1170251,    1404289,    1685119,    2022103,    2426521,    2911819,    3494177,    4192997,    5031583,
    6037861,    7245409,    8694481,    10433377,   12520051,   15024049,   18028847,   21634607,   25961521,
    31153823,   37384573,   44861483,   53833757,   64600507,   77520607,   93024709,   111629641,  133955531,
    160746617,  192895931,  231475073,  277769959,  333323929,  399988711,  479986421,  575983697,  691180421,
    829416503,  995299787,  1194359743, 1433231687, 1719878023, 2063853629, 2476624327, 2971949179, 3566339011,
    4279606787, 5135528143, 6162633733, 7395160471};

static uint32_t PRIME_NUMBER_COUNT = sizeof(PRIME_NUMBER_TABLE) / sizeof(PRIME_NUMBER_TABLE[0]);

PrimeNumberTable::PrimeNumberTable() {}

PrimeNumberTable::~PrimeNumberTable() {}

int64_t PrimeNumberTable::FindPrimeNumber(int64_t num)
{
    assert(num <= PRIME_NUMBER_TABLE[PRIME_NUMBER_COUNT - 1]);
    assert(num >= 0);

    int64_t* begin = PRIME_NUMBER_TABLE;
    int64_t* end = begin + PRIME_NUMBER_COUNT;
    int64_t* iter = std::lower_bound(begin, end, num);
    return *iter;
}
}} // namespace indexlib::util
