package org.apache.beam.examples;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Team
 */
@DefaultCoder(AvroCoder.class)
public class Team {

    private double payroll;
    private double wins;
    private String name;

    public Team(String name, double payroll, double wins){
        this.payroll=payroll;
        this.wins=wins;
        this.name=name;        
    }

    public Team(){}

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public double getPayroll() {
        return payroll;
    }
    public double getWins() {
        return wins;
    }
    public void setPayroll(double payroll) {
        this.payroll = payroll;
    }
    public void setWins(double wins) {
        this.wins = wins;
    }
    @Override
    public String toString() {
        return "Team Name: "+this.name+" Payroll Milions: "+this.payroll+" Wins: "+this.wins;
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
        
        Team team = (Team) o;
        return this.name.equals(team.getName()) && this.payroll == team.getPayroll() && this.wins == team.getWins();
        }
    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + name.hashCode();
        result = 31 * result + (int)wins;
        result = 31 * result + (int)payroll;
        return result;
    }

}    
    

 