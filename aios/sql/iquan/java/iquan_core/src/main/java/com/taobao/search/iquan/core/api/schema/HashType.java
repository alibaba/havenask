package com.taobao.search.iquan.core.api.schema;

public enum HashType {
    // hash type define:
    // /ha3_develop/source_code/autil/autil/HashFuncFactory.cpp#HashFuncFactory::createHashFunc
    HF_HASH(Constant.HASH),
    HF_HASH64(Constant.HASH64),
    HF_GALAXY_HASH(Constant.GALAXY_HASH),
    HF_KINGSO_HASH(Constant.KINGSO_HASH),
    HF_NUMBER_HASH(Constant.NUMBER_HASH),
    HF_ROUTING_HASH(Constant.ROUTING_HASH),
    HF_SMARTWAVE_HASH(Constant.SMARTWAVE_HASH),
    HF_INVALID_HASH(Constant.INVALID);

    private final String name;

    HashType(String name) {
        this.name = name;
    }

    public static HashType from(String name) {
        name = name.toUpperCase();

        // issue: try to slove the case, if the user set the hash type as "KINGSO_HASH#720"
        if (name.startsWith(Constant.NUMBER_HASH)) {
            return HF_NUMBER_HASH;
        } else if (name.startsWith(Constant.KINGSO_HASH)) {
            return HF_KINGSO_HASH;
        } else if (name.startsWith(Constant.GALAXY_HASH)) {
            return HF_GALAXY_HASH;
        } else if (name.startsWith(Constant.HASH64)) {
            return HF_HASH64;
        } else if (name.startsWith(Constant.HASH)) {
            return HF_HASH;
        } else if (name.startsWith(Constant.ROUTING_HASH)) {
            return HF_ROUTING_HASH;
        } else if (name.startsWith(Constant.SMARTWAVE_HASH)) {
            return HF_SMARTWAVE_HASH;
        } else {
            return HF_INVALID_HASH;
        }
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public boolean isValid() {
        return this != HF_INVALID_HASH;
    }

    public interface Constant {
        String HASH = "HASH";
        String HASH64 = "HASH64";
        String GALAXY_HASH = "GALAXY_HASH";
        String KINGSO_HASH = "KINGSO_HASH";
        String NUMBER_HASH = "NUMBER_HASH";
        String ROUTING_HASH = "ROUTING_HASH";
        String SMARTWAVE_HASH = "SMARTWAVE_HASH";
        String INVALID = "INVALID";
    }
}
