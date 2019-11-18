package com.test.spark.wiki.extracts.domains;

import lombok.Builder;

@Builder
public class HeartBeat implements FormationBean{
    String appName;
    Long sentTime ;


    public HeartBeat(String appName, Long sentTime) {
        this.appName = appName;
        this.sentTime = sentTime;
    }

    public HeartBeat() {
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public Long getSentTime() {
        return sentTime;
    }

    public void setSentTime(Long sentTime) {
        this.sentTime = sentTime;
    }

    @Override
    public String getTopic() {
       return FormationConfig.HEARTBEAT_TOPIC;
    }
}
