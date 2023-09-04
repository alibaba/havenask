#include <map>

#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

std::map<jobject, int> ReferenceCnt;
template <typename T>
T makeInstance(int v) {
    T ref = reinterpret_cast<T>(v);
    ReferenceCnt[ref]++;
    return ref;
}
class TestAlloc {
public:
    static constexpr const char *name = "TestAlloc";
    jobject ref(jobject original) const {
        ReferenceCnt[original]++;
        return original;
    }
    void unref(jobject reference) const noexcept {
        ReferenceCnt[reference]--;
    }
};

std::map<jobject, int> ReferenceCnt1;
template <typename T>
T makeInstance1(int v) {
    T ref = reinterpret_cast<T>(v);
    ReferenceCnt1[ref]++;
    return ref;
}
class TestAlloc1 {
public:
    static constexpr const char *name = "TestAlloc1";
    jobject ref(jobject original) const {
        ReferenceCnt1[original]++;
        return original;
    }
    void unref(jobject reference) const noexcept {
        ReferenceCnt1[reference]--;
    }
};

template <typename T>
using TRef = StrongRef<T, TestAlloc>;

class ReferencesTest : public IquanTestBase {
public:
    virtual void setUp() override;
};

void ReferencesTest::setUp() {
    ReferenceCnt.clear();
    ReferenceCnt1.clear();
}

TEST_F(ReferencesTest, testGetSet) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        StrongRef<JString, TestAlloc> t;
        ASSERT_EQ(nullptr, t.get());

        t.set(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testGetSet2) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        StrongRef<JString, TestAlloc> t;
        ASSERT_EQ(nullptr, t.get());

        t.set(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        t.set(o1);
        ASSERT_EQ(o1, t.get());
        ASSERT_EQ(0, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testRelease) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        StrongRef<JString, TestAlloc> t;
        ASSERT_EQ(nullptr, t.get());

        t.set(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        jstring o2 = t.release();
        ASSERT_EQ(o, o2);
        ASSERT_EQ(nullptr, t.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testReset) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t;
        ASSERT_EQ(nullptr, t.get());

        t.set(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        t.reset();
        ASSERT_EQ(nullptr, t.get());
        ASSERT_EQ(0, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testConstructor) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testCopyConstructor) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());

        ASSERT_EQ(2, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JObject, TestAlloc> t2(t1);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(o, t2.get());

        ASSERT_EQ(3, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testCopyConstructorOtherAlloc) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
        ASSERT_EQ(0, ReferenceCnt1[o]);
        ASSERT_EQ(0, ReferenceCnt1[o1]);

        StrongRef<JString, TestAlloc1> t1(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
        ASSERT_EQ(1, ReferenceCnt1[o]);
        ASSERT_EQ(0, ReferenceCnt1[o1]);

        StrongRef<JObject, TestAlloc1> t2(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(o, t2.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
        ASSERT_EQ(2, ReferenceCnt1[o]);
        ASSERT_EQ(0, ReferenceCnt1[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    ASSERT_EQ(0, ReferenceCnt1[o]);
    ASSERT_EQ(0, ReferenceCnt1[o1]);
}

TEST_F(ReferencesTest, testMoveConstructor) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1(std::move(t));
        ASSERT_EQ(nullptr, t.get());
        ASSERT_EQ(o, t1.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JObject, TestAlloc> t2(std::move(t1));
        ASSERT_EQ(nullptr, t1.get());
        ASSERT_EQ(o, t2.get());

        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testCopyConstructorFromAlias) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        AliasRef<JString> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(2, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }

    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAssignment) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1(o1);
        ASSERT_EQ(o1, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        t1 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(2, ReferenceCnt[o]);
        ASSERT_EQ(0, ReferenceCnt[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAssignmentOtherAlloc) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance1<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt[o1]);
    ASSERT_EQ(0, ReferenceCnt1[o]);
    ASSERT_EQ(1, ReferenceCnt1[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(0, ReferenceCnt[o1]);
        ASSERT_EQ(0, ReferenceCnt1[o]);
        ASSERT_EQ(1, ReferenceCnt1[o1]);

        StrongRef<JString, TestAlloc1> t1(o1);
        ASSERT_EQ(o1, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(0, ReferenceCnt[o1]);
        ASSERT_EQ(0, ReferenceCnt1[o]);
        ASSERT_EQ(1, ReferenceCnt1[o1]);

        t1 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(0, ReferenceCnt[o1]);
        ASSERT_EQ(1, ReferenceCnt1[o]);
        ASSERT_EQ(0, ReferenceCnt1[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt[o1]);
    ASSERT_EQ(0, ReferenceCnt1[o]);
    ASSERT_EQ(0, ReferenceCnt1[o1]);
}

TEST_F(ReferencesTest, testMoveAssignment) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1(o1);
        ASSERT_EQ(o1, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        t1 = std::move(t);
        ASSERT_EQ(nullptr, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(0, ReferenceCnt[o1]);
    }

    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAssignmentFromAlias) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);

    {
        AliasRef<JString> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        StrongRef<JString, TestAlloc> t1;
        ASSERT_EQ(nullptr, t1.get());
        t1 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(2, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }

    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefGetSet) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        AliasRef<JString> t;
        ASSERT_EQ(nullptr, t.get());

        t.set(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        t.set(o1);
        ASSERT_EQ(o1, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefConstructor) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        AliasRef<JString> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefCopyConstructor) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        AliasRef<JString> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JString> t1(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JObject> t2(t1);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ((jobject)o, t2.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefCopyConstructorFromStrong) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JString> t1(t);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefAssignment) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        AliasRef<JString> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JString> t1;
        ASSERT_EQ(nullptr, t1.get());
        t1 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JObject> t2;
        ASSERT_EQ(nullptr, t2.get());
        t2 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t2.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

TEST_F(ReferencesTest, testAliasRefAssignmentFromStrong) {
    jstring o = makeInstance<jstring>(100);
    jstring o1 = makeInstance<jstring>(101);
    ASSERT_EQ(1, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
    {
        StrongRef<JString, TestAlloc> t(o);
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JString> t1;
        ASSERT_EQ(nullptr, t1.get());
        t1 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t1.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);

        AliasRef<JObject> t2;
        ASSERT_EQ(nullptr, t2.get());
        t2 = t;
        ASSERT_EQ(o, t.get());
        ASSERT_EQ(o, t2.get());
        ASSERT_EQ(1, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt[o1]);
    }
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(1, ReferenceCnt[o1]);
}

StrongRef<JString, TestAlloc> getARef(int v) {
    jstring o = makeInstance<jstring>(v);
    return {o};
}

TEST_F(ReferencesTest, testRefAssignmentFromFunction) {
    int v = 100;
    jstring o;
    {
        StrongRef<JString, TestAlloc> t;
        t = getARef(v);
        o = t.get();
        ASSERT_EQ(reinterpret_cast<jstring>(v), o);
        ASSERT_EQ(1, ReferenceCnt[o]);
    }
    ASSERT_EQ(reinterpret_cast<jstring>(v), o);
    ASSERT_EQ(0, ReferenceCnt[o]);
    {
        StrongRef<JString, TestAlloc1> t;
        t = getARef(v);
        o = t.get();
        ASSERT_EQ(reinterpret_cast<jstring>(v), o);
        ASSERT_EQ(0, ReferenceCnt[o]);
        ASSERT_EQ(1, ReferenceCnt1[o]);
    }
    ASSERT_EQ(reinterpret_cast<jstring>(v), o);
    ASSERT_EQ(0, ReferenceCnt[o]);
    ASSERT_EQ(0, ReferenceCnt1[o]);
}

} // namespace iquan
