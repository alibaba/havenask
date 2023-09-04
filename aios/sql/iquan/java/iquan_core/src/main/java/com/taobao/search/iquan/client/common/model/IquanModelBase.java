package com.taobao.search.iquan.client.common.model;

import com.taobao.search.iquan.client.common.common.ConstantDefine;

public abstract class IquanModelBase {
    protected long id = 0;
    protected String gmt_create = ConstantDefine.EMPTY;
    protected String gmt_modified = ConstantDefine.EMPTY;
    protected String catalog_name = ConstantDefine.EMPTY;
    protected String database_name = ConstantDefine.EMPTY;
    protected int status = 1;

    abstract boolean isQualifierValid();

    abstract boolean isPathValid();

    abstract boolean isValid();

    abstract String getDigest();

    @Override
    public String toString() {
        return getDigest();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getGmt_create() {
        return gmt_create;
    }

    public void setGmt_create(String gmt_create) {
        this.gmt_create = gmt_create;
    }

    public String getGmt_modified() {
        return gmt_modified;
    }

    public void setGmt_modified(String gmt_modified) {
        this.gmt_modified = gmt_modified;
    }

    public String getCatalog_name() {
        return catalog_name;
    }

    public void setCatalog_name(String catalog_name) {
        this.catalog_name = catalog_name;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
