package models;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.mapping.Id;
import play.modules.cassandra.Model;
import play.modules.cassandra.annotations.Entity;

import java.util.UUID;

@Entity(columnFamily = "ACCOUNT")
public class Account extends Model {
    @Id("GUID")
    public String guid;

    @Column("XP")
    public Long xp;

    @Column("COINS")
    public Long coins;

    public Account()
    {
        this.guid = UUID.randomUUID().toString();
    }

    public String toString()
    {
        return String.format("Account[guid=%s,xp=%d,coins=%d]", guid, xp, coins);
    }
}
