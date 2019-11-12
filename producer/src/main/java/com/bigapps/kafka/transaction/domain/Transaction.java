package com.bigapps.kafka.transaction.domain;

import java.io.Serializable;

public class Transaction implements Serializable {
    private String id;
    private double amount;

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

    public Transaction(String id, double amount) {
        this.id = id;
        this.amount = amount;
    }
}
