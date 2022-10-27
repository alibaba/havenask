#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/common/sparsehash/config.h>
#include <ha3/common/sparsehash/dense_hash_map>
#include <tr1/unordered_map>
#include <random>
#include <sys/time.h>

using GOOGLE_NAMESPACE::dense_hash_map;
using GOOGLE_NAMESPACE::libc_allocator_with_realloc;

using namespace std;
using namespace testing;

class DenseMapTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    std::vector<int> _dataset;
};

void DenseMapTest::setUp() {
    std::random_device rd;
    std::mt19937_64 mt(rd());
    std::uniform_int_distribution<int> dist(1, 10001);
    for (uint64_t i = 0; i < 1000000; i++) {
        _dataset.push_back(dist(mt));
    }
}

void DenseMapTest::tearDown() {
}

template <class Alloc, class key_type, class value_type, class EqualKey>
class MyEmptyDelLogic {
private:
    typedef typename Alloc::template rebind<value_type>::other value_alloc_type;
public:
    typedef typename value_alloc_type::size_type size_type;
    typedef typename value_alloc_type::pointer pointer;
public:
    bool test_deleted(const size_type num_deleted, const key_type &delkey, const key_type &key) const {
        return false;
    }
    bool test_empty(const key_type &empty_key, const key_type &key, const value_type &val) const {
        return val.second == 0;
    }
};

typedef libc_allocator_with_realloc<std::pair<const int, int> > Alloc;
typedef dense_hash_map<int, int, SPARSEHASH_HASH<int>, std::equal_to<int>,
                       Alloc,
                       MyEmptyDelLogic<Alloc, int, std::pair<const int, int>, std::equal_to<int> > >
my_dense_hash_map;

TEST_F(DenseMapTest, testDenseMapFunc) {
    my_dense_hash_map ht;
    std::pair<int, int> empty(0,0);
    ht.set_empty_key_value(empty);
    // never insert empty kv
    ht[0] = 0;
    ht[1] = 1;

    auto iter = ht.find(0);
    ASSERT_TRUE(ht.size() == 2);
    ASSERT_TRUE(iter == ht.end());
    uint32_t count = 0;
    for (iter = ht.begin(); iter != ht.end(); ++iter) {
        ++count;
    }
    ASSERT_TRUE(count == 1);
    ASSERT_TRUE(ht[0] == 0);
    ASSERT_EQ(ht[1], 1);
}

TEST_F(DenseMapTest, testDenseMap) {
    std::tr1::unordered_map<int, int> um(32);
    my_dense_hash_map ht;
    std::pair<int, int> empty(0,0);
    ht.set_empty_key_value(empty);
    struct timeval t1, t2;
    // dense_hash_map
    cout << "dense_hash_map bucket_count: " << ht.bucket_count() << endl;
    gettimeofday(&t1, NULL);
    for (int i : _dataset) {
        auto it = ht.find(i);
        if (it != ht.end()) {
            ++it->second;
        } else {
            ht[i] = 1;
        }
    }
    gettimeofday(&t2, NULL);
    cout << "dense_hash_map time :"
         << (t2.tv_sec - t1.tv_sec) * 1000 + (t2.tv_usec - t1.tv_usec) / 1000.0
         << endl;
    // unordered_map
    cout << "unordered_map capacity: " << um.bucket_count() << endl;
    gettimeofday(&t1, NULL);
    for (int i : _dataset) {
        auto it = um.find(i);
        if (it != um.end()) {
            ++it->second;
        } else {
            um[i] = 1;
        }
    }
    gettimeofday(&t2, NULL);
    cout << "unordered_map time :"
         << (t2.tv_sec - t1.tv_sec) * 1000 + (t2.tv_usec - t1.tv_usec) / 1000.0
         << endl;

    for (int i = 1; i < 10001; i++) {
        if (ht.find(i) != ht.end()) {
            ASSERT_TRUE(ht[i] == um[i]);
        }
    }
}
