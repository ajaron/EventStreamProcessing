package com.stormpractice.eventhandler;

import java.util.Random;
import java.util.UUID;

public class Event {

    private String userId;
    private String ipAddress;
    private UUID eventId;

    private int countForUser;

    public int getCountForUser() {
        return countForUser;
    }

    public void setCountForUser(int countForUser) {
        this.countForUser = countForUser;
    }

    public int getCountForIp() {
        return countForIp;
    }

    public void setCountForIp(int countForIp) {
        this.countForIp = countForIp;
    }

    private int countForIp;

    //Used to generate a random number
    static Random _rand = new Random();

    public static Event genEvent() {
        return new Event(UUID.randomUUID(), genUserId(), genIpAddress() );
    }


    public Event(UUID eventId, String userId, String ipAddress) {
        this.eventId = eventId;
        this.userId = userId;
        this.ipAddress = ipAddress;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }


    private static String genIpAddress() {

        return "192.168.27." + _rand.nextInt(70);
    }

    private static String genUserId() {

        return  Integer.toString(_rand.nextInt(70));
    }




}
