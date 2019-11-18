package com.test.spark.wiki.extracts.domains;

import lombok.*;

@Builder
public class Transaction implements FormationBean{
    private String id;
    private double amount;
    private String type;
    private Long date;
    private String account;

    @Override
    public String getTopic() {
        return FormationConfig.TRANSACTION_TOPIC;
    }

    public Transaction(){}

    public Transaction(String id, double amount, String type, Long date, String account) {
        this.id = id;
        this.amount = amount;
        this.type = type;
        this.date = date;
        this.account = account;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

}
