package org.apache.beam.examples;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
/**
 * Player
 */

@DefaultCoder(AvroCoder.class)
public class Player {

    private String name;
    private String team;
    private String position;
    private double height;
    private double weight;
    private double age;

    public Player(String name, String team, String position, double height, double weight, double age) {
        this.name = name;
        this.team = team;
        this.position = position;
        this.height = height;
        this.weight = weight;
        this.age = age;
    }

    public Player() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getAge() {
        return age;
    }

    public void setAge(double age) {
        this.age = age;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    @Override
    public String toString() {
        return "Player Name: " + this.name + " Team: " + this.team + " Position: " + this.position + " Height: "
                + this.height + " Weight: " + this.weight + " Age: " + this.age;
    }

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o)
            return true;
        // null check
        if (o == null)
            return false;
        // type check and cast
        if (getClass() != o.getClass())
            return false;

        Player player = (Player) o;
        return this.name.equals(player.getName()) && this.team.equals(player.getTeam())
                && this.weight == player.getWeight() && this.height == player.getHeight()
                && this.position.equals(player.getPosition()) && this.age == player.getAge();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + name.hashCode();
        result = 31 * result + (int) height;
        result = 31 * result + (int) weight;
        return result;
    }

}
